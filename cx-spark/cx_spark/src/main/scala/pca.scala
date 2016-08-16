package org.apache.spark.mllib.linalg.distributed

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.{Matrices, DenseMatrix, Matrix, DenseVector, Vector, SparseVector}
import org.apache.spark.mllib.linalg.EigenValueDecomposition
import org.apache.spark.mllib.linalg.distributed._

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, Axis, qr, svd, sum, SparseVector => BSV}
import breeze.linalg.{norm, eigSym, diag, accumulate}

//import edu.berkeley.cs.amplab.mlmatrix.{RowPartitionedMatrix, TSQR}

import math.{ceil, log, sqrt}

import spray.json._
import DefaultJsonProtocol._
import java.util.Arrays
import java.io.{DataOutputStream, BufferedOutputStream, FileOutputStream, File}
import org.apache.spark.sql.{SQLContext, Row => SQLRow}

//import grizzled.slf4j.Logger

object PCAvariants { 

  def fromBreeze(mat: BDM[Double]): DenseMatrix = {
    // FIXME: does not support strided matrices (e.g. views)
    new DenseMatrix(mat.rows, mat.cols, mat.data, mat.isTranspose)
  }

  def report(message: String, verbose: Boolean = false) = {
    if(verbose) {
      println("STATUS REPORT: " + message)
    }
  }

  def getRowMean(mat: IndexedRowMatrix) = {
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
  }

  def sampledIdentityColumns(n : Int, numsamps: Int, probs: BDV[Double]) = {
    val rng = new java.util.Random
    val observations = List.fill(numsamps)(rng.nextDouble)
    val colCumProbs = accumulate(probs).toArray.zipWithIndex
    val keepIndices = observations.map( u => colCumProbs.find(_._1 >= u).getOrElse(Tuple2(1.0, n))._2.asInstanceOf[Int] ).toArray
    val omega = BDM.zeros[Double](n, numsamps)
    for( j <- 0 until numsamps) {
      omega(keepIndices(j), j) = 1  
    }
    omega
  }

  // Returns `(1/(numrowsofmat) * mat.transpose * mat - avg*avg.transpose) * rhs`
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
    fromBreeze(1.0/mat.numRows * result - avg * tmp.t)
  }

  /* get low-rank approximation to the covariance matrix for input using randomized PCA
   * algorithm. The rows of mat are the observations, 
   */
  def computeRPCA(mat: IndexedRowMatrix, rank: Int, slack: Int, numIters: Int,
    verbose: Boolean = false) : Tuple3[BDV[Double], BDM[Double], BDV[Double]] = {
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

    /* get the approximate top PCs by taking the SVD of an implicitly formed 
     * low-rank approximation to the covariance matrix
     * Q*Q.T * Cov * Q*Q.t
     */
    report("performing EVD", verbose)
    // manual resymmetrization should not be necessary with the next release of Breeze:
    // the current one is too sensitive, so you have to do this
    var B = 0.5 * Q.t * multiplyCovarianceBy(mat, fromBreeze(Q), rowavg).toBreeze.asInstanceOf[BDM[Double]] 
    B += B.t
    // NB: eigSym(B) gives U and d such that U*diag(d)*U^T = B, and the entries of d are in order of smallest to largest
    // so d is in the reverse of the usual mathematical order
    val eigSym.EigSym(lambdatilde, utilde) = eigSym(B)
    report("done performing EVD", verbose)

    if (k > rank) {
      val lambda = lambdatilde(k-1 until k-1-rank by -1)
      val U = (Q*utilde).apply(::, k-1 until k-1-rank by -1).copy
      (lambda, U, rowavg)
    } else {
      val lambda = lambdatilde((0 until k).reverse)
      val U = (Q*utilde).apply(::, (0 until k).reverse).copy
      (lambda, U, rowavg)
     }
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

  def dumpMat(outf: DataOutputStream, mat: BDM[Double]) = {
    outf.writeInt(mat.rows)
    outf.writeInt(mat.cols)
    for(i <- 0 until mat.rows) {
      for(j <- 0 until mat.cols) {
        outf.writeDouble(mat(i,j))
      }
    }
  }

  def dumpV(outf: DataOutputStream, v: BDV[Double]) = {
    outf.writeInt(v.length) 
    for(i <- 0 until v.length) {
      outf.writeDouble(v(i))
    }
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
    val nparts = if(args.length >= 9) { args(8).toInt } else { 0 }
    val mat: IndexedRowMatrix = loadMSIData(sc, matkind, shape, inpath, nparts)
    mat.rows.cache()

    // force materialization of the RDD so we can separate I/O from compute
    mat.rows.count()

    // do the RPCA
    val rpcaResults = computeRPCA(mat, rank, slack, numIters, true)
    val rpcaEvals = rpcaResults._1
    val rpcaEvecs = rpcaResults._2
    val mean = rpcaResults._3

    //compute the CX (on centered data, this just uses the evecs from RPCA)
    val levProbs = sum(rpcaEvecs :^ 2.0, Axis._1) / rank.toDouble
    report("Sampling the identity matrix according to leverage scores", true)
    val sampleMat = sampledIdentityColumns(mat.numCols.toInt, rank, levProbs)
    report("Done sampling", true)
    report("Sampling the covariance matrix columns", true)
    val colsMat = multiplyCovarianceBy(mat, fromBreeze(sampleMat), mean).toBreeze.asInstanceOf[BDM[Double]]
    report("Done sampling columns", true)
    report("Taking the QR of the columns", true)
    val qr.QR(q, r) = qr.reduced(colsMat)
    report("Done with QR", true)
    val cxQ = q
    report("Getting X in CX decomposition", true)
    val xMat = r \ multiplyCovarianceBy(mat, fromBreeze(cxQ), mean).toBreeze.asInstanceOf[BDM[Double]].t 
    report("Done forming X", true)

    //do the PCA
    val tol = 1e-10 // using same value as in MLLib
    val maxIter = 30 // was 300, but it actually used this many iterations, too much time
    val covOperator = ( v: BDV[Double] ) =>  multiplyCovarianceBy(mat, fromBreeze(v.toDenseMatrix).transpose, mean).toBreeze.asInstanceOf[BDM[Double]].toDenseVector
    report("Using ARPACK to form the EVD of the covariance operator", true)
    val (lambda2, u2) = EigenValueDecomposition.symmetricEigs(covOperator, mat.numCols.toInt, rank, tol, maxIter)
    report("Done with EVD of covariance operator", true)
    val pcaEvals = lambda2
    val pcaEvecs = u2

    // estimate the relative Frobenius norm reconstruction errors
    // to save memory, generate the product with the random matrix and calculate its Frobenius norm in blocks of columns
    report("Computing the Frobenius norm relative errors", true)

    var estFrobNorm : Double = 0.0
    var rpcaEstFrobNormErr : Double = 0.0
    var pcaEstFrobNormErr : Double = 0.0
    var cxEstFrobNormErr : Double = 0.0

    val rng = new java.util.Random
    val numSamps = 1000 // 1000 samples at once seem to cause an out of memory error at the treeAggregate call from two lines below, why? this is only 4*1000*132000/(1024^3) ~ .5G
    val chunkSize = 100
    var omegaPartial = DenseMatrix.zeros(mat.numCols.toInt, chunkSize)
    var matrixProdPartial = BDM.zeros[Double](mat.numCols.toInt, chunkSize)
    var diffPartial = BDM.zeros[Double](mat.numCols.toInt, chunkSize)

    for( i <- 1 to numSamps/chunkSize) {
      omegaPartial = DenseMatrix.randn(mat.numCols.toInt, numSamps, rng)

      // accumulate the Frobenius norm estimate for the covariance matrix
      matrixProdPartial = multiplyCovarianceBy(mat, omegaPartial, mean).toBreeze.asInstanceOf[BDM[Double]]
      estFrobNorm = estFrobNorm + 1/numSamps.toDouble * sum(matrixProdPartial :* matrixProdPartial) 

      // accumulate the RPCA Frobenius norm error estimate
      diffPartial = matrixProdPartial - rpcaEvecs*(diag(rpcaEvals)*rpcaEvecs.t*omegaPartial.toBreeze.asInstanceOf[BDM[Double]])
      rpcaEstFrobNormErr = rpcaEstFrobNormErr + 1/numSamps.toDouble * sum(diffPartial :* diffPartial)

      // accumulate the PCA Frobenius norm error estimate
      diffPartial = matrixProdPartial - pcaEvecs*(diag(pcaEvals)*pcaEvecs.t*omegaPartial.toBreeze.asInstanceOf[BDM[Double]])
      pcaEstFrobNormErr = pcaEstFrobNormErr + 1/numSamps.toDouble * sum(diffPartial :* diffPartial)

      //accumulate the CX Frobenius norm error estimate
      diffPartial = matrixProdPartial - colsMat * (xMat * omegaPartial.toBreeze.asInstanceOf[BDM[Double]])
      cxEstFrobNormErr = cxEstFrobNormErr + 1/numSamps.toDouble * sum(diffPartial :* diffPartial)
    }
    estFrobNorm = sqrt(estFrobNorm)
    rpcaEstFrobNormErr = sqrt(rpcaEstFrobNormErr)
    pcaEstFrobNormErr = sqrt(pcaEstFrobNormErr)
    cxEstFrobNormErr = sqrt(cxEstFrobNormErr)

    report("Done computing the Frobenius norm errors", true)
    report(f"RPCA estimated relative frobenius norm err ${rpcaEstFrobNormErr/estFrobNorm}", true)
    report(f"PCA estimated relative frobenius norm err ${pcaEstFrobNormErr/estFrobNorm}", true)
    report(f"CX estimated relative frobenius norm err ${cxEstFrobNormErr/estFrobNorm}", true)
    
    // write output 
    val outf = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(outpath))))
    report("Writing parameters and decompositions to file", true)
    outf.writeInt(shape._1)
    outf.writeInt(shape._2)
    outf.writeInt(rank)
    outf.writeInt(slack)
    outf.writeInt(numIters)
    outf.writeInt(nparts)
    outf.writeDouble(estFrobNorm)
    outf.writeDouble(rpcaEstFrobNormErr)
    outf.writeDouble(pcaEstFrobNormErr)
    outf.writeDouble(cxEstFrobNormErr)
    dumpV(outf, mean)
    dumpV(outf, rpcaEvals)
    dumpMat(outf, rpcaEvecs)
    dumpV(outf, pcaEvals)
    dumpMat(outf, pcaEvecs)
    dumpMat(outf, colsMat)
    dumpMat(outf, xMat)
    outf.close()
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("pcavariants")
    conf.set("spark.task.maxFailures", "1")
    val sc = new SparkContext(conf)

    appMain(sc, args)
  }

}
