package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Matrices, DenseMatrix, Matrix, DenseVector, Vector, SparseVector, Vectors}
import org.apache.spark.mllib.linalg.EigenValueDecomposition
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Axis, qr, svd, sum, SparseVector => BSV}
import breeze.linalg.{norm, diag, accumulate}
import breeze.numerics.{sqrt => BrzSqrt}
import math.{ceil, log}
import scala.collection.mutable.ArrayBuffer

import spray.json._
import DefaultJsonProtocol._
import java.util.Arrays
import java.io.{DataOutputStream, BufferedOutputStream, FileOutputStream, File}
import java.util.Calendar
import java.text.SimpleDateFormat

import org.apache.spark.AccumulatorParam

object BDMAccumulatorParam extends AccumulatorParam[BDM[Double]] {
  def zero(initialValue: BDM[Double]): BDM[Double] = {
    BDM.zeros[Double](initialValue.rows, initialValue.cols) 
  }

  def addInPlace(m1: BDM[Double], m2: BDM[Double]) : BDM[Double] = {
    m1 += m2
  }
}

object SVDVariants {
  def fromBreeze(mat: BDM[Double]): DenseMatrix = {
    // FIXME: does not support strided matrices (e.g. views)
    new DenseMatrix(mat.rows, mat.cols, mat.data, mat.isTranspose)
  }

  def report(message: String, verbose: Boolean = false) = {
    val now = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat("H:m:s")

    if(verbose) {
      println("STATUS REPORT (" + formatter.format(now) + "): " + message)
    }
  }

  def sampleCenteredColumns(mat : IndexedRowMatrix, numsamps: Int, probs: BDV[Double], rowavg: BDV[Double]) = {
    val rng = new java.util.Random
    val observations = List.fill(numsamps)(rng.nextDouble)
    val colCumProbs = accumulate(probs).toArray.zipWithIndex
    val keepIndices = observations.map( u => colCumProbs.find(_._1 >= u).getOrElse(Tuple2(1.0, mat.numCols.toInt))._2.asInstanceOf[Int] ).toArray

    def sampleFromRow(row : IndexedRow) = {
      val rowBrz = row.vector.toBreeze.asInstanceOf[BSV[Double]]
      val valBuffer = ArrayBuffer.empty[Double]
      for(indexpos <- keepIndices) {
        valBuffer += rowBrz(indexpos) - rowavg(indexpos)
      }
      new IndexedRow(row.index, new DenseVector(valBuffer.toArray))
    }
    // ugly hack to collect the matrix
    fromBreeze(new IndexedRowMatrix(mat.rows.map( row => sampleFromRow(row)), 
      mat.numRows, numsamps).toBreeze)
  }

  def sampleCenteredRows(mat: IndexedRowMatrix, numsamps: Int, probs: BDV[Double], rowavg: BDV[Double]) = {
    val rng = new java.util.Random
    val observations = List.fill(numsamps)(rng.nextDouble)
    val rowCumProbs = accumulate(probs).toArray.zipWithIndex
    val keepIndices = observations.map( u => rowCumProbs.find(_._1 >= u).getOrElse(Tuple2(1.0, mat.numRows.toInt))._2.asInstanceOf[Int] ).toArray
    
    val neededrows = mat.rows.filter(row => keepIndices.contains(row.index)).map(row => (row.index, row.vector.toBreeze.asInstanceOf[BSV[Double]])).collect()
    
    // stupid hack: how can you make a DenseMatrix out of a list of vectors? 
    var result = BDM.zeros[Double](numsamps, mat.numCols.toInt)
    for(rowpos <- 0 until numsamps) {
       val currow = neededrows.find(_._1 == keepIndices(rowpos)).getOrElse(neededrows(0))._2
       for( jpos <- 0 until currow.index.length) {
         val j = currow.index(jpos)
         val v = currow.data(jpos)
         result(rowpos, j) = v
       }
    }
    fromBreeze(result)
  }

  def multiplyCenteredMatBy(mat: IndexedRowMatrix, rhs: DenseMatrix, avg: BDV[Double]) : DenseMatrix = {
    def centeredRow( row : Vector) : Vector = {
      val temp = (row.toBreeze.asInstanceOf[BSV[Double]] - avg).toArray
      Vectors.dense(temp)
    }
    var rowmat = new IndexedRowMatrix( mat.rows.map(row => new IndexedRow(row.index, centeredRow(row.vector))),
      mat.numRows, mat.numCols.toInt)
    // hack to collapse distributed matrix to a local DenseMatrix
    fromBreeze(rowmat.multiply(rhs).toBreeze)
  }

  // Returns `(1/numobs * mat.transpose * mat - avg*avg.transpose) * rhs`
  def multiplyCovarianceBy(mat: IndexedRowMatrix, rhs: DenseMatrix, avg: BDV[Double]): DenseMatrix = {
    val rhsBrz = rhs.toBreeze.asInstanceOf[BDM[Double]]
    val result =
      mat.rows.treeAggregate(BDM.zeros[Double](mat.numCols.toInt, rhs.numCols))(
        seqOp = (U: BDM[Double], row: IndexedRow) => {
          val rowBrz = row.vector.toBreeze.asInstanceOf[BSV[Double]]
          val tmp: BDV[Double] = rhsBrz.t * rowBrz
          // performs a rank-1 update:
          //   U += outer(row.vector, tmp)
          for(ipos <- 0 until rowBrz.index.length) {
            val i = rowBrz.index(ipos)
            val ival = rowBrz.data(ipos)
            for(j <- 0 until tmp.length) {
              U(i, j) += ival * tmp(j)
            }
          }
          U
        },
        combOp = (U1, U2) => U1 += U2
      )
    val tmp = rhsBrz.t * avg
    fromBreeze(1/mat.numRows.toDouble * result - avg * tmp.t)
  }

  /*
  // computes BA where B is a local matrix and A is distributed: let b_i denote the
  // ith col of B and a_i denote the ith row of A, then BA = sum(b_i a_i)
  def leftMultiplyCenteredMatrixBy(mat: IndexedRowMatrix, lhs: DenseMatrix, avg: BDV[Double]) : DenseMatrix = {
     val lhsFactor = mat.rows.context.broadcast(lhs.toBreeze.asInstanceOf[BDM[Double]])

     val result = mat.rows.map(row => {
         val lhs = lhsFactor.value
         lhs(::, row.index.toInt) * (row.vector.toBreeze.asInstanceOf[BSV[Double]] - avg).t
       }).reduce(_ + _)

     fromBreeze(result)
    }

  def leftMultiplyCenteredMatrixBy(mat: IndexedRowMatrix, lhs: DenseMatrix, avg: BDV[Double]) : DenseMatrix = {
  
   //report("lhsfactor size: " + lhs.numRows.toInt + "x" + lhs.numCols.toInt, true)

   val lhsBrz = lhs.toBreeze.asInstanceOf[BDM[Double]]
   val lhsList = List.tabulate(lhs.numCols.toInt)(i => lhsBrz(::, i))
   val accum = mat.rows.context.accumulator(BDM.zeros[Double](lhs.numRows.toInt, mat.numCols.toInt))(BDMAccumulatorParam)

   mat.rows.foreach(row => accum += lhsList(row.index.toInt) * (row.vector.toBreeze.asInstanceOf[BSV[Double]] - avg).t)

   fromBreeze(accum.value)
  }
  */

  // computes BA where B is a local matrix and A is distributed: let b_i denote the
  // ith col of B and a_i denote the ith row of A, then BA = sum(b_i a_i)
  def leftMultiplyCenteredMatrixBy(mat: IndexedRowMatrix, lhs: DenseMatrix, avg: BDV[Double]) : DenseMatrix = {
   val lhsFactor = mat.rows.context.broadcast(lhs.toBreeze.asInstanceOf[BDM[Double]])

   val result = 
     mat.rows.treeAggregate(BDM.zeros[Double](lhs.numRows.toInt, mat.numCols.toInt))(
       seqOp = (U: BDM[Double], row: IndexedRow) => {
         val rowBrz = row.vector.toBreeze.asInstanceOf[BSV[Double]] - avg
         U += (lhsFactor.value)(::, row.index.toInt) * rowBrz.t
       },
       combOp = (U1, U2) => U1 += U2, depth = 8
     )
   fromBreeze(result)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("SVDVariants")
    conf.set("spark.task.maxFailures", "1").set("spark.executor.memory", "64G").set("spark.driver.memory", "64G")
    val sc = new SparkContext(conf)
    appMain(sc, args)
  }

  // hack to avoid centering without writing a lot more code
  def getRowMean(mat: IndexedRowMatrix) = {
    BDV.zeros[Double](mat.numCols.toInt)
    /*
    1.0/mat.numRows * mat.rows.treeAggregate(BDV.zeros[Double](mat.numCols.toInt))(
      seqOp = (avg: BDV[Double], row: IndexedRow) => {
        val rowBrz = row.vector.toBreeze.asInstanceOf[BSV[Double]]
        for (ipos <- 0 until rowBrz.index.length) {
          val idx = rowBrz.index(ipos)
          val rval = rowBrz.data(ipos)
          avg(idx) += rval
        }
        avg
      },
      combOp = (avg1, avg2) => avg1 += avg2
      )
    */
  }

  def loadMSIData(sc: SparkContext, matkind: String, shape: Tuple2[Int, Int], inpath: String, nparts: Int = 0) = {

      val sqlctx = new org.apache.spark.sql.SQLContext(sc)
      import sqlctx.implicits._

      val mat0: IndexedRowMatrix = 
        if(matkind == "csv") {
          /* weird way to input data:
           * We assume the data is stored as an m-by-n matrix, with each
           * observation as a column
           * we pass in the matrix dimensions as (n,m) = shape
           * the following code loads the data into an n-by-m matrix
           * so that the observations are now rows of the matrix
           */
          val nonzeros = sc.textFile(inpath).map(_.split(",")).
          map(x => new MatrixEntry(x(1).toLong, x(0).toLong, x(2).toDouble))
          val coomat = new CoordinateMatrix(nonzeros, shape._1, shape._2)
          val mat = coomat.toIndexedRowMatrix()
          mat
        } else if(matkind == "idxrow") {
          val rows = sc.objectFile[IndexedRow](inpath)
          new IndexedRowMatrix(rows, shape._1, shape._2)
        } else if(matkind == "df") {
          val numRows = if(shape._1 != 0) shape._1 else sc.textFile(inpath + "/rowtab.txt").count.toInt
          val numCols = if(shape._2 != 0) shape._2 else sc.textFile(inpath + "/coltab.txt").count.toInt
          val rows =
            sqlctx.parquetFile(inpath + "/matrix.parquet").rdd.map {
              case SQLRow(index: Long, vector: Vector) =>
                new IndexedRow(index, vector)
            }
          new IndexedRowMatrix(rows, numRows, numCols)
        } else {
          throw new RuntimeException(s"unrecognized matkind: $matkind")
        }
        
      if (nparts == 0) {
        mat0
      } else {
        new IndexedRowMatrix(mat0.rows.coalesce(nparts), mat0.numRows, mat0.numCols.toInt) 
      }
  }

  /* get low-rank approximation to the centered input matrix using randomized SVD
   * algorithm. The rows of mat are the observations, 
   */
  def computeRSVD(mat: IndexedRowMatrix, rank: Int, slack: Int, numIters: Int,
    verbose: Boolean = false) : Tuple4[BDM[Double], BDV[Double], BDM[Double], BDV[Double]] = {
    val rng = new java.util.Random
    val k = rank + slack
    var Y = DenseMatrix.randn(mat.numCols.toInt, k, rng).toBreeze.asInstanceOf[BDM[Double]]
    report("Computing mean of observations", verbose)
    val rowavg = getRowMean(mat)
    report("Done computing mean of observations", verbose)

    report("performing iterations", verbose)
    for(i <- 0 until numIters) {
      Y = multiplyCovarianceBy(mat, fromBreeze(Y), rowavg).toBreeze.asInstanceOf[BDM[Double]]
      Y = qr.reduced.justQ(Y)
    }
    report("done iterations", verbose)

    report("performing QR to find basis", verbose)
    val Q = qr.reduced.justQ(Y)
    report("done performing QR", verbose)
    assert(Q.cols == k)

    report("performing SVD on A*Q", verbose)
    var B = multiplyCenteredMatBy(mat, fromBreeze(Q), rowavg).toBreeze.asInstanceOf[BDM[Double]].t
    val Bsvd = svd.reduced(B)
    // Since we compute the randomized SVD of A', unswap U and V here to get back
    // svd(A) = U*S*V'
    val V = (Q * Bsvd.U).apply(::, 0 until rank)
    val S = Bsvd.S(0 until rank)
    val U = Bsvd.Vt(0 until rank, ::).t
    report("done performing SVD", verbose)

    // Don't care about ordering so skip the reordering necessary to get the
    // singular values in increasing order
    /*
    if (k > rank) {
      val lambda = S(k-1 until k-1-rank by -1)
      val Usorted = U(::, k-1 until k-1-rank by -1).copy
      val Vsorted = V(::, k-1 until k-1-rank by -1).copy
      (Usorted, lambda, Vsorted, rowavg)
    } else {
      val lambda = S((0 until k).reverse)
      val Usorted = U(::, (0 until k).reverse).copy
      val Vsorted = V(::, (0 until k).reverse).copy
      (Usorted, lambda, Vsorted, rowavg)
     }
     */
    (U, S, V, rowavg)
  }

  def appMain(sc: SparkContext, args: Array[String]) = {
    if(args.length < 8) {
      Console.err.println("Expected args: [csv|idxrow|df] inpath nrows ncols outpath rank slack niters [nparts]")
      System.exit(1)
    }

    val matkind = args(0)
    val inpath = args(1)
    val shape = (args(2).toInt, args(3).toInt)
    val outpath = args(4)

    // rank of approximation
    val rank = args(5).toInt

    // extra slack to improve the approximation
    val slack = args(6).toInt

    // number of power iterations to perform
    val numIters = args(7).toInt

    //input the data
    val nparts = if(args.length >= 9) { args(8).toInt } else {0}
    val mat: IndexedRowMatrix = loadMSIData(sc, matkind, shape, inpath, nparts)
    mat.rows.cache()

    // force materialization of the RDD so we can separate I/O from compute
    mat.rows.count()

    /*
    //try to force error to occur
    report("Number of partition of input matrix: " + mat.rows.partitions.size, true)

    val testtall = BDM.rand[Double](shape._1, rank)
    val testfat = BDM.rand[Double](rank, shape._2)
    val testmean = BDV.zeros[Double](shape._2)
    val test_colCXFrobNormErr = calcCenteredFrobNormErr(mat, testtall, testfat, testmean)
    */

    /* perform randomized SVD of centered A' */
    val rsvdResults = computeRSVD(mat, rank, slack, numIters, true)
    val rsvdU = rsvdResults._1
    val rsvdSingVals = rsvdResults._2
    val rsvdV = rsvdResults._3
    val mean = rsvdResults._4

    // compute the CX (on centered data, this uses the singvecs from PCA); use column selection
    val colLevProbs = sum(rsvdV :^ 2.0, Axis._1) / rank.toDouble
    report("Sampling the columns according to leverage scores", true)
    val colsMat = sampleCenteredColumns(mat, rank, colLevProbs, mean).toBreeze.asInstanceOf[BDM[Double]]
    report("Done sampling columns", true)
    report("Taking the QR of the columns", true)
    val qr.QR(q, r) = qr.reduced(colsMat)
    report ("Done with QR", true)
    val cxQ = q
    report("Size of temporary array: " + fromBreeze(cxQ.t).numRows.toInt + "x" + mat.numCols.toInt, true)
    report("Getting X in the column CX decomposition", true)
    val cxMat = r \ leftMultiplyCenteredMatrixBy(mat, fromBreeze(cxQ.t), mean).toBreeze.asInstanceOf[BDM[Double]]
    report("Done forming X", true)

    // compute the CX (on centered data, this uses the singvecs from PCA); use row selection
    val rowLevProbs = sum(rsvdU :^ 2.0, Axis._1) / rank.toDouble
    report("Sampling the rows according to leverage scores", true)
    val rowsMat = sampleCenteredRows(mat, rank, rowLevProbs, mean).toBreeze.asInstanceOf[BDM[Double]]
    report("Done sampling rows", true)
    report("Taking the QR of the rows", true)
    val qr.QR(q2, r2) = qr.reduced(rowsMat.t)
    report("Done with QR", true)
    val rxQ = q2
    report("Getting X in the row CX decomposition", true)
    val rxMat = r2 \ multiplyCenteredMatBy(mat, fromBreeze(rxQ), mean).toBreeze.asInstanceOf[BDM[Double]].t
    report("Done forming X", true)

    // compute the truncated SVD by using PCA to get the right singular vectors
    // todo: port PROPACK here
    val tol = 1e-11
    val maxIter = 100
    val covOperator = (v : BDV[Double]) => multiplyCovarianceBy(mat, fromBreeze(v.toDenseMatrix).transpose, mean).toBreeze.asInstanceOf[BDM[Double]].toDenseVector
    report("Done with centered RSVD and centered CX, now computing truncated centered SVD", true)
    // find PCs first using EVD, then project data unto these and recompute the SVD
    report("Computing truncated EVD of covariance operator", true)
    val (lambda2, u2) = EigenValueDecomposition.symmetricEigs(covOperator, mat.numCols.toInt, rank, tol, maxIter)
    report("Done with truncated EVD of covariance operator", true)
    report("Extracting left singular vectors", true)
    val firstSVD = svd.reduced(multiplyCenteredMatBy(mat, fromBreeze(u2), mean).toBreeze.asInstanceOf[BDM[Double]])
    val secondSVD = svd.reduced(diag(firstSVD.S)*firstSVD.Vt*u2.t)
    val tsvdU = firstSVD.U * secondSVD.U
    val tsvdSingVals = secondSVD.S
    val tsvdV = secondSVD.Vt.t
    report("Done extracting left singular vectors", true)

    report("Computing the Frobenius norm relative errors of approximations of centered matrix", true)

    var frobNorm : Double = 0.0
    var rsvdFrobNormErr : Double = 0.0
    var colCXFrobNormErr : Double = 0.0
    var rowCXFrobNormErr : Double = 0.0
    var tsvdFrobNormErr : Double = 0.0

    // calcCenteredFrobNormErr keeps dieing: think it may be because one executor is unbalanced, so timesout and gets killed?
    frobNorm = math.sqrt(mat.rows.map(row => math.pow(norm(row.vector.toBreeze.asInstanceOf[BSV[Double]] - mean), 2)).reduce( (x:Double, y: Double) => x + y))
    rsvdFrobNormErr = calcCenteredFrobNormErr(mat, rsvdU, diag(rsvdSingVals) * rsvdV.t ,mean)
    colCXFrobNormErr = calcCenteredFrobNormErr(mat, colsMat, cxMat, mean)
    rowCXFrobNormErr = calcCenteredFrobNormErr(mat, rxMat.t, rowsMat, mean)
    tsvdFrobNormErr = calcCenteredFrobNormErr(mat, tsvdU, diag(tsvdSingVals)*tsvdV.t, mean)

    report(f"Frobenius norm of centered matrix: ${frobNorm}", true)
    report(f"RSVD relative Frobenius norm error: ${rsvdFrobNormErr/frobNorm}", true)
    report(f"column CX relative Frobenius norm error: ${colCXFrobNormErr/frobNorm}", true)
    report(f"row CX relative Frobenius norm error: ${rowCXFrobNormErr/frobNorm}", true)
    report(f"TSVD relative Frobenius norm error: ${tsvdFrobNormErr/frobNorm}", true)

    val outf = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outpath))))
    report("Writing parameters and approximation errors to file", true)
    outf.writeInt(shape._1)
    outf.writeInt(shape._2)
    outf.writeInt(rank)
    outf.writeInt(slack)
    outf.writeInt(numIters)
    outf.writeInt(nparts)
    outf.writeDouble(frobNorm)
    outf.writeDouble(rsvdFrobNormErr)
    outf.writeDouble(colCXFrobNormErr)
    outf.writeDouble(rowCXFrobNormErr)
    outf.writeDouble(tsvdFrobNormErr)
    outf.close()
  }

  /*
  def calcCenteredFrobNormErr(mat: IndexedRowMatrix, lhsTall: BDM[Double], rhsFat: BDM[Double], mean: BDV[Double] ) : Double = {
    val lhsFactor = mat.rows.context.broadcast(lhsTall)
    val rhsFactor = mat.rows.context.broadcast(rhsFat)

    def partitionDiffFrobNorm2(rowiter : Iterator[IndexedRow], lhsFactor: Broadcast[BDM[Double]], rhsFactor: Broadcast[BDM[Double]]) : Iterator[Double] = {

      val lhsTall = lhsFactor.value
      val rhsFat = rhsFactor.value

      val rowlist = rowiter.toList
      val numrows = rowlist.length
      val matSubMat = BDM.zeros[Double](numrows, mat.numCols.toInt)
      val lhsSubMat = BDM.zeros[Double](numrows, lhsTall.cols)

      var currowindex = 0
      rowlist.foreach( 
        (currow: IndexedRow) => {
          currow.vector.foreachActive { case (j, v) => matSubMat(currowindex, j) = v }
          lhsSubMat(currowindex, ::) := lhsTall(currow.index.toInt, ::)
          currowindex += 1
        }
      )

      val diffmat = matSubMat - lhsSubMat * rhsFat - BDV.ones[Double](matSubMat.rows)*mean.t
      List(sum(diffmat :* diffmat)).iterator
    }

    report("Beginning to compute Frobenius norm", true)
    val res = mat.rows.mapPartitions(rowiter => partitionDiffFrobNorm2(rowiter, lhsFactor, rhsFactor)).reduce(_ + _)
    report("Finished computing Frobenius norm", true)
    math.sqrt(res)
  }

  def calcCenteredFrobNormErr(mat: IndexedRowMatrix, lhsTall: BDM[Double], rhsFat: BDM[Double], mean: BDV[Double]) : Double = {
    val sc = mat.rows.context
    val lhsFactor = sc.broadcast(lhsTall)
    val rhsFactor = sc.broadcast(rhsFat)

    math.sqrt(mat.rows.map( row => {
        val lhs = lhsFactor.value
        val rhs = rhsFactor.value
        math.pow(norm( row.vector.toBreeze.toDenseVector - mean - (lhs(row.index.toInt, ::) * rhs).t.toDenseVector ), 2)
      }).reduce(_ + _))
  }
*/

  def calcCenteredFrobNormErr(mat: IndexedRowMatrix, lhsTall: BDM[Double], rhsFat: BDM[Double], mean: BDV[Double]) : Double = {
    val sse = 
      mat.rows.treeAggregate(BDV.zeros[Double](1))(
        seqOp = (partial: BDV[Double], row: IndexedRow) => {
          val reconstructed = (lhsTall(row.index.toInt, ::) * rhsFat).t
          partial(0) += math.pow(norm(row.vector.toBreeze.asInstanceOf[BSV[Double]] - mean - reconstructed), 2)
          partial
        },
        combOp = (partial1, partial2) => partial1 += partial2,
        depth = 8
      )
    math.sqrt(sse(0))
  }

  def dump(outf: DataOutputStream, v: BDV[Double]) = {
    outf.writeInt(v.length)
    for(i <- 0 until v.length) {
      outf.writeDouble(v(i))
    }
  }
  def loadMatrixA(sc: SparkContext, fn: String) = {
    val input = scala.io.Source.fromFile(fn).getLines()
    require(input.next() == "%%MatrixMarket matrix coordinate real general")
    val dims = input.next().split(' ').map(_.toInt)
    val seen = BDM.zeros[Int](dims(0), dims(1))
    val entries = input.map(line => {
      val toks = line.split(" ")
      val i = toks(0).toInt - 1
      val j = toks(1).toInt - 1
      val v = toks(2).toDouble
      require(toks.length == 3)
      new MatrixEntry(i, j, v)
    }).toSeq
    require(entries.length == dims(2))
    new CoordinateMatrix(sc.parallelize(entries, 1), dims(0), dims(1)).toIndexedRowMatrix
  }

  def loadMatrixB(fn: String) = {
    val input = scala.io.Source.fromFile(fn).getLines()
    require(input.next() == "%%MatrixMarket matrix coordinate real general")
    val dims = input.next().split(' ').map(_.toInt)
    val seen = BDM.zeros[Int](dims(0), dims(1))
    val result = BDM.zeros[Double](dims(0), dims(1))
    var count = 0
    input.foreach(line => {
      val toks = line.split(" ")
      require(toks.length == 3)
      val i = toks(0).toInt - 1
      val j = toks(1).toInt - 1
      val v = toks(2).toDouble
      require(i >= 0 && i < dims(0))
      require(j >= 0 && j < dims(1))
      require(seen(i, j) == 0)
      seen(i, j) = 1
      result(i, j) = v
      if(v != 0) count += 1
    })
    //assert(count == dims(2))
    fromBreeze(result)
  }

  def writeMatrix(mat: DenseMatrix, fn: String) = {
    val writer = new java.io.FileWriter(new java.io.File(fn))
    writer.write("%%MatrixMarket matrix coordinate real general\n")
    writer.write(s"${mat.numRows} ${mat.numCols} ${mat.numRows*mat.numCols}\n")
    for(i <- 0 until mat.numRows) {
      for(j <- 0 until mat.numCols) {
        writer.write(f"${i+1} ${j+1} ${mat(i, j)}%f\n")
      }
    }
    writer.close
  }
}
