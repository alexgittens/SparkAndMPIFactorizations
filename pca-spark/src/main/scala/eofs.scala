/* Computes the EOFs of large climate datasets with no missing data
 *
 */

package org.apache.spark.mllib.climate

import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.nersc.io.read
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Matrices, DenseMatrix, Matrix, DenseVector, Vector, SparseVector, Vectors}

import scala.io.Source

//import org.apache.spark.mllib.linalg.EigenValueDecomposition
import org.apache.spark.mllib.linalg.distributed._
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Axis, qr, svd, sum, SparseVector => BSV}
import breeze.linalg.{norm, diag, accumulate, rank => BrzRank}
import breeze.linalg.{min, argmin}
import breeze.stats.meanAndVariance
import breeze.numerics.{sqrt => BrzSqrt}
import math.{ceil, log}
import scala.collection.mutable.ArrayBuffer

import java.util.Arrays
import java.io.{DataOutputStream, BufferedOutputStream, FileOutputStream, File}
import java.util.zip.GZIPOutputStream
import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.hadoop.io.compress.DefaultCodec
import org.nersc.io._

import org.msgpack.MessagePack;

object computeEOFs {

  def report(message: String, verbose: Boolean = true) = {
    val now = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat("H:m:s")

    if(verbose) {
      println("STATUS REPORT (" + formatter.format(now) + "): " + message)
    }
  }

  case class EOFDecomposition(leftVectors: DenseMatrix, singularValues: DenseVector, rightVectors: DenseMatrix) {
    def U: DenseMatrix = leftVectors
    def S: DenseVector = singularValues
    def V: DenseMatrix = rightVectors
  }

  def fromBreeze(mat: BDM[Double]): DenseMatrix = {
    new DenseMatrix(mat.rows, mat.cols, mat.data, mat.isTranspose)
  }

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("ClimateEOFs")
    //conf.set("spark.task.maxFailures", "1").set("spark.shuffle.blockTransferService", "nio")
    val sc = new SparkContext(conf)
    sys.addShutdownHook( { sc.stop() } )
    appMain(sc, args)
  }

  def appMain(sc: SparkContext, args: Array[String]) = {
    val inpath = args(0) //a text file with the format path/to/h5/file dataset-name partitions
    val numrows = args(1).toInt
    val numcols = args(2).toLong
    val preprocessMethod = args(3)
    val numeofs = args(4).toInt
    val outdest = args(5)
    val randomizedq = args(6)
    //val variable = args(7)
    //val repartition = args(8).toLong
    val oversample = 5
    val powIters = 10
    
    val info = loadH5ClimateData(sc, inpath, numrows, numcols, preprocessMethod)
    val mat = info.productElement(0).asInstanceOf[IndexedRowMatrix]

    val (u,v) = 
    if ("exact" == randomizedq) {
      getLowRankFactorization(mat, numeofs)
    } else if ("randomized" == randomizedq) {
      getApproxLowRankFactorization(mat, numeofs, oversample, powIters)
    } else if ("both" == randomizedq) {
      val (u1,v1) = getLowRankFactorization(mat, numeofs)
      val (u2,v2) = getApproxLowRankFactorization(mat, numeofs, oversample, powIters)
      val exactEOFs = convertLowRankFactorizationToEOFs(u1.asInstanceOf[DenseMatrix], v1.asInstanceOf[DenseMatrix])
      val inexactEOFs = convertLowRankFactorizationToEOFs(u2.asInstanceOf[DenseMatrix], v2.asInstanceOf[DenseMatrix])
      writeOut(outdest + "exact", exactEOFs, info)
      writeOut(outdest + "inexact", inexactEOFs, info)
      sc.stop()
      System.exit(0)
    }

    //should be minimal here but we will time it
    val convertStart = System.currentTimeMillis
    val climateEOFs = convertLowRankFactorizationToEOFs(u.asInstanceOf[DenseMatrix], v.asInstanceOf[DenseMatrix])
    //val convertTime = System.currentTimeMillis - convertStart
    println("LocalCompute: Convert EOF Time = " + (System.currentTimeMillis - convertStart))
    // Only uncomment if we need the EOFs, otherwise we really only care about timing information
    //writeOutBasic(outdest, climateEOFs, info)

    report(s"U - ${climateEOFs.U.numRows}-by-${climateEOFs.U.numCols}")
    report(s"S - ${climateEOFs.S.size}")
    report(s"V - ${climateEOFs.V.numRows}-by-${climateEOFs.V.numCols}")

  }

  // returns the processed matrix as well as a Product containing the information relevant to that processing:
  //
  def loadH5ClimateData(sc: SparkContext, inpath: String, numrows: Int, numcols: Long, preprocessMethod: String) : Product = {
   /*
    val sqlctx = new org.apache.spark.sql.SQLContext(sc)
    import sqlctx.implicits._

    val rows = {
      sqlctx.parquetFile(inpath + "/finalmat.parquet").rdd.map {
        case SQLRow(index: Long, vector: Vector) =>
          new IndexedRow(index, vector)
      }
    }//.repartition(2880)
    //rows.persist(StorageLevel.MEMORY_ONLY_SER)
    val tempmat = new IndexedRowMatrix(rows, numcols, numrows)
    */

    //val tempmat = read.h5read_imat (sc,inpath, variable, repartition)
    val lines = Source.fromFile(inpath).getLines.toArray
    val params1 = lines(0).split(" ")
    val partitions = params1(2).toLong
    var rdd = read.h5read (sc,params1(0),params1(1),params1(2).toLong)
    for (i <- 1 to lines.length -1) {
      val params = lines(i).split(" ")
      val rdd_temp = read.h5read(sc,params(0),params(1),params(2).toLong)
      rdd = rdd.union(rdd_temp)
    }
    // coalesces it to the partitions that the first line requested b/c otherwise it will add all the partitions
    val new_rdd = rdd.coalesce(partitions.toInt)
    //rdd.cache()
    val irow = new_rdd.zipWithIndex().map( k  => (k._1, k._2)).map(k => new IndexedRow(k._2, k._1))
    val tempmat = new IndexedRowMatrix(irow)

    if ("centerOverAllObservations" == preprocessMethod) {
      val mean = getRowMeans(tempmat)
      val centeredmat = subtractMean(tempmat, mean)
      centeredmat.rows.persist(StorageLevel.MEMORY_ONLY_SER)
      centeredmat.rows.count()
      //rows.unpersist()

      (centeredmat, mean)
    }else if ("standardizeEach" == preprocessMethod) {
      val (mean, stdv) = getRowMeansAndStd(tempmat)
      val standardizedmat = standardize(tempmat, mean, stdv)
      standardizedmat.rows.persist(StorageLevel.MEMORY_ONLY_SER)
      standardizedmat.rows.count()
      //rows.unpersist()

      (standardizedmat, mean, stdv)
    }else if ("cosLat+centerOverAllObservations" == preprocessMethod) {
      val mean = getRowMeans(tempmat)
      val centeredmat = subtractMean(tempmat, mean)

      val latitudeweights = BDV.zeros[Double](tempmat.numRows.toInt)
      sc.textFile(inpath + "/latitudeweights.csv").map( line => line.split(",") ).collect.map( pair => latitudeweights(pair(0).toInt) = pair(1).toDouble )

      val reweightedmat = areaWeight(centeredmat, latitudeweights)
      reweightedmat.rows.persist(StorageLevel.MEMORY_ONLY_SER)
      reweightedmat.rows.count()
      //rows.unpersist()

      (reweightedmat, areaWeight(mean, latitudeweights))
    } else {
      None
    }
  }

  def areaWeight(mat: IndexedRowMatrix, weights: BDV[Double]) : IndexedRowMatrix = {
    val reweightedrows = mat.rows.map(x => new IndexedRow(x.index, new DenseVector((x.vector.toBreeze.asInstanceOf[BDV[Double]] * weights(x.index.toInt)).toArray)) )
    new IndexedRowMatrix(reweightedrows, mat.numRows, mat.numCols.toInt)
  }

  def areaWeight(vec: BDV[Double], weights: BDV[Double]) : BDV[Double] = {
    vec :* weights
  }

  def calcSSE(mat: IndexedRowMatrix, lhsTall: BDM[Double], rhsFat: BDM[Double]) : Double = {
      val sse = mat.rows.treeAggregate(BDV.zeros[Double](1))(
        seqOp = (partial: BDV[Double], row: IndexedRow) => {
          val reconstructed = (lhsTall(row.index.toInt, ::) * rhsFat).t
          partial(0) += math.pow(norm(row.vector.toBreeze.asInstanceOf[BDV[Double]] - reconstructed), 2)
          partial
        },
        combOp = (partial1, partial2) => partial1 += partial2,
        depth = 5
      )
      sse(0)
  }

  // a la https://gist.github.com/thiagozs/6699612
  def writeOut(outdest: String, eofs: EOFDecomposition, info: Product) {
    val outf = new GZIPOutputStream(new FileOutputStream(outdest))
    val msgpack = new MessagePack();
    val packer = msgpack.createPacker(outf)

    packer.write(eofs.U.numCols) // number of EOFs
    packer.write(eofs.U.numRows) // number of observation points
    packer.write(eofs.U.toBreeze.asInstanceOf[BDM[Double]].toDenseVector.toArray)
    packer.write(eofs.V.toBreeze.asInstanceOf[BDM[Double]].toDenseVector.toArray)
    packer.write(eofs.S.toBreeze.asInstanceOf[BDV[Double]].toArray)
    packer.write(info.productElement(1).asInstanceOf[BDV[Double]].toArray)
    if (info.productArity == 3) {
      packer.write(info.productElement(2).asInstanceOf[BDV[Double]].toArray) 
    }
  
    outf.close()
  }

  def writeOutBasic(outdest: String, eofs: EOFDecomposition, info: Product) {
    val outf = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outdest))))
    dumpMat(outf, eofs.U.toBreeze.asInstanceOf[BDM[Double]])
    dumpMat(outf, eofs.V.toBreeze.asInstanceOf[BDM[Double]])
    dumpV(outf, eofs.S.toBreeze.asInstanceOf[BDV[Double]])
    dumpV(outf, info.productElement(1).asInstanceOf[BDV[Double]])
    if (info.productArity == 3) {
      dumpV(outf, info.productElement(2).asInstanceOf[BDV[Double]])
    }
    outf.close()
  }

  def dumpV(outf: DataOutputStream, v: BDV[Double]) = {
    outf.writeInt(v.length) 
    for(i <- 0 until v.length) {
      outf.writeDouble(v(i))
    }
  }

  def dumpVI(outf: DataOutputStream, v: BDV[Int]) = {
    outf.writeInt(v.length) 
    for(i <- 0 until v.length) {
      outf.writeInt(v(i))
    }
  }

  def dumpMat(outf: DataOutputStream, mat: BDM[Double]) = {
    outf.writeInt(mat.rows)
    outf.writeInt(mat.cols)
    for(i <- 0 until mat.rows) {
      for(j <- 0 until mat.cols) {
        outf.writeDouble(mat(i,j))
      }
    }
  }

  // returns a column vector of the means of each row
  def getRowMeans(mat: IndexedRowMatrix) : BDV[Double] = {
    val scaling = 1.0/mat.numCols
    val mean = BDV.zeros[Double](mat.numRows.toInt)
    val meanpairs = mat.rows.map( x => (x.index, scaling * x.vector.toArray.sum)).collect()
    //report(s"Computed ${meanpairs.length} row means (presumably nonzero)")
    meanpairs.foreach(x => mean(x._1.toInt) = x._2)
    mean
  }

  // returns two column vectors: one of the means of the row, one of the standard deviation
  // for now, use a two-pass algorithm (one-pass can be unstable)
  def getRowMeansAndStd(mat: IndexedRowMatrix) : Tuple2[BDV[Double], BDV[Double]] = {
    val mean = BDV.zeros[Double](mat.numRows.toInt)
    val std = BDV.zeros[Double](mat.numRows.toInt)
    val meanstdpairs = mat.rows.map( x => (x.index, meanAndVariance(x.vector.toBreeze.asInstanceOf[BDV[Double]])) ).collect()
    //report(s"Computed ${meanstdpairs.length} row means and stdvs (presumably nonzero)")
    meanstdpairs.foreach( x => { mean(x._1.toInt) = x._2.mean; std(x._1.toInt) = x._2.stdDev })
    val stdtol = .0001
    val badpairs = std.toArray.zipWithIndex.filter(x => x._1 < stdtol)
    report(s"Indices of rows with too low standard deviation, and the standard deviations (there are ${badpairs.length} such rows):")
    report(s"${badpairs.map(x => x._2).foldLeft("")( (x, y) => x + " " + y.toString )}")
    badpairs foreach { case(badstd, idx) => report(s"(${idx}, ${badstd})") }
    report(s"Replacing those bad standard deviations with 1s")
    badpairs.foreach(x => std(x._2) = 1)
    (mean, std)
  }

  def subtractMean(mat: IndexedRowMatrix, mean: BDV[Double]) : IndexedRowMatrix = {
    val centeredrows = mat.rows.map(x => new IndexedRow(x.index, new DenseVector(x.vector.toArray.map(v => v - mean(x.index.toInt)))))
    new IndexedRowMatrix(centeredrows, mat.numRows, mat.numCols.toInt)
  }

  def standardize(mat: IndexedRowMatrix, mean: BDV[Double], stdv: BDV[Double]) : IndexedRowMatrix = {
    val standardizedrows = mat.rows.map(x => new IndexedRow(x.index, new DenseVector(x.vector.toArray.map( v => 1./(stdv(x.index.toInt)) * (v - mean(x.index.toInt)) ))))
    new IndexedRowMatrix(standardizedrows, mat.numRows, mat.numCols.toInt)
  }

  def convertLowRankFactorizationToEOFs(u : DenseMatrix, v : DenseMatrix) : EOFDecomposition = {
    val vBrz = v.toBreeze.asInstanceOf[BDM[Double]]
    val vsvd = svd.reduced(vBrz)
    // val diff = vsvd.U*diag(BDV(vsvd.S.data))*fromBreeze(vsvd.Vt.t).toBreeze.asInstanceOf[BDM[Double]].t - vBrz
    // report(s"Sanity check (should be 0): ${diff.data.map(x => x*x).sum}")
    EOFDecomposition(fromBreeze(u.toBreeze.asInstanceOf[BDM[Double]] * vsvd.U), new DenseVector(vsvd.S.data), fromBreeze(vsvd.Vt.t))
  }

  // returns U V with k columns so that U*V.t is an optimal rank-k approximation to mat and U has orthogonal columns
  def getLowRankFactorization(mat: IndexedRowMatrix, rank: Int) : Tuple2[DenseMatrix, DenseMatrix] = {
    val tol = 1e-13
    val maxIter = 30
    val covOperator = ( v: BDV[Double] ) => multiplyCovarianceBy(mat, fromBreeze(v.toDenseMatrix).transpose).toBreeze.asInstanceOf[BDM[Double]].toDenseVector
    val (lambda, u) = EigenValueDecomposition.symmetricEigs(covOperator, mat.numCols.toInt, rank, tol, maxIter)
    //report(s"Square Frobenius norm of approximate row basis for data: ${u.data.map(x=>x*x).sum}")
    val Xlowrank = mat.multiply(fromBreeze(u)).toBreeze()
    val qrStartTime = System.currentTimeMillis
    val qr.QR(q,r) = qr.reduced(Xlowrank)
    println("Local Compute: Breeze QR Time = " + (System.currentTimeMillis - qrStartTime))

    //report(s"Square Frobenius norms of Q,R: ${q.data.map(x=>x*x).sum}, ${r.data.map(x=>x*x).sum}") 
    (fromBreeze(q), fromBreeze(r*u.t)) 
  }

  // returns U V with k columns so that U*V.t is a low-rank approximate factorization to mat and U has orthogonal columns
  // uses the RSVD algorithm
  def getApproxLowRankFactorization(mat: IndexedRowMatrix, rank: Int, oversample: Int, numIters: Int) : Tuple2[DenseMatrix, DenseMatrix] = {
    val rng = new java.util.Random()
    var Y = DenseMatrix.randn(mat.numCols.toInt, rank + oversample, rng)
    for( iterIdx <- 0 until numIters) {
      val Ynew = multiplyCovarianceByV2(mat, Y)
      val qr.QR(q,r) = qr.reduced(Ynew.toBreeze.asInstanceOf[BDM[Double]])
      Y = fromBreeze(q)
    }
    // refine the approximation to the top right singular vectors of the covariance matrix:
    // rather than taking the first columns from the extended basis Y, take the svd of Y
    // and use its top singular vectors
    val tempsvd = svd.reduced(Y.toBreeze.asInstanceOf[BDM[Double]])
    val V = tempsvd.U(::, 0 until rank).copy
    val Xlowrank = mat.multiply(fromBreeze(V)).toBreeze()
    val qr.QR(q,r) = qr.reduced(Xlowrank)
    (fromBreeze(q), fromBreeze(r*V.t))
  }

  // computes BA where B is a local matrix and A is distributed: let b_i denote the
  // ith col of B and a_i denote the ith row of A, then BA = sum(b_i a_i)
  def leftMultiplyBy(mat: IndexedRowMatrix, lhs: DenseMatrix) : DenseMatrix = {
   report(s"Left multiplying a ${mat.numRows}-by-${mat.numCols} matrix by a ${lhs.numRows}-by-${lhs.numCols} matrix")
   val lhsFactor = mat.rows.context.broadcast(lhs.toBreeze.asInstanceOf[BDM[Double]])

   val result =
     mat.rows.treeAggregate(BDM.zeros[Double](lhs.numRows.toInt, mat.numCols.toInt))(
       seqOp = (U: BDM[Double], row: IndexedRow) => {
         val rowBrz = row.vector.toBreeze.asInstanceOf[BDV[Double]]
         U += (lhsFactor.value)(::, row.index.toInt) * rowBrz.t
       },
       combOp = (U1, U2) => U1 += U2, depth = 3
     )
   fromBreeze(result)
  }

  // Returns `1/n * mat.transpose * mat * rhs`
  def multiplyCovarianceBy(mat: IndexedRowMatrix, rhs: DenseMatrix): DenseMatrix = {
    report(s"Going to multiply the covariance operator of a ${mat.numRows}-by-${mat.numCols} matrix by a ${rhs.numRows}-by-${rhs.numCols} matrix")
    val rhsBrz = rhs.toBreeze.asInstanceOf[BDM[Double]]
    val result = 
      mat.rows.treeAggregate(BDM.zeros[Double](mat.numCols.toInt, rhs.numCols))(
        seqOp = (U: BDM[Double], row: IndexedRow) => {
          val rowBrz = row.vector.toBreeze.asInstanceOf[BDV[Double]]
          U += row.vector.toBreeze.asInstanceOf[BDV[Double]].asDenseMatrix.t * (row.vector.toBreeze.asInstanceOf[BDV[Double]].asDenseMatrix * rhs.toBreeze.asInstanceOf[BDM[Double]])
        },
        combOp = (U1, U2) => U1 += U2
      )
    fromBreeze(1.0/mat.numRows * result)
  }

  def multiplyCovarianceByV2(mat: IndexedRowMatrix, rhs: DenseMatrix): DenseMatrix = {
    val rhsBrz = rhs.toBreeze.asInstanceOf[BDM[Double]]
    val numCols = rhsBrz.rows

    def outerProduct(iter: Iterator[IndexedRow]) : Iterator[BDM[Double]] = {
      val mat = BDM.zeros[Double](iter.length, numCols)
      for (rowidx <- 0 until iter.length) {
        val currow = iter.next.vector.toBreeze.asInstanceOf[BDV[Double]].t
        mat(rowidx, ::) := currow
      }
      val chunk = mat.t * (mat * rhsBrz)
      return List(chunk).iterator
    }

    val result = mat.rows.mapPartitions(outerProduct).treeReduce(_ + _)
    fromBreeze(1.0/mat.numRows * result)
  }
}
