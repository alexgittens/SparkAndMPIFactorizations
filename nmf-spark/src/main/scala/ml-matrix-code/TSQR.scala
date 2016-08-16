package edu.berkeley.cs.amplab.mlmatrix

import java.util.concurrent.ThreadLocalRandom
import scala.collection.mutable.ArrayBuffer

import breeze.linalg._

import edu.berkeley.cs.amplab.mlmatrix.util.QRUtils
import edu.berkeley.cs.amplab.mlmatrix.util.Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.Accumulator
import org.apache.spark.SparkContext._

import java.util.Calendar
import java.text.SimpleDateFormat

class modifiedTSQR extends Serializable {

    def report(message: String, verbose: Boolean = true) = {
        val now = Calendar.getInstance().getTime()
        val formatter = new SimpleDateFormat("H:m:s")
        if (verbose) {
            println("STATUS REPORT (" + formatter.format(now) + "): " + message)
        }
    }

  /**
   * Returns only the R factor of a QR decomposition of the input matrix.
   * The value of R is not reproducible across multiple invocations of the function
   */
  def qrR(mat: RowPartitionedMatrix): Tuple2[DenseVector[Double], DenseMatrix[Double]] = {
    val localQR = mat.rdd.context.accumulator(0.0, "Time taken for Local QR")

    // assumes the matrix is non-negative
    def coll1norms(mat : DenseMatrix[Double]) : DenseVector[Double] = {
        sum(mat, Axis._0).t(::,0)
    }

    val qrTree = mat.rdd.map { part =>
      if (part.mat.rows < part.mat.cols) {
        (coll1norms(part.mat), part.mat)
      } else {
        report("begin local QR computation")
        val begin = System.nanoTime
        val r = QRUtils.qrR(part.mat)
        report("finish local QR computation")
        localQR += ((System.nanoTime - begin) / 1000000)
        (coll1norms(part.mat), r)
      }
    }

    val depth = math.ceil(
      math.log(math.max(mat.rdd.partitions.size, 2.0)) / math.log(2)).toInt
    Utils.treeReduce(qrTree, reduceQR(localQR, _ : Tuple2[DenseVector[Double], DenseMatrix[Double]],
        _ : Tuple2[DenseVector[Double], DenseMatrix[Double]]), depth=depth)
  }

  /**
   * Concatenates two matrices in a commutative reduce operation
   * and then returns the R factor of the QR factorization of the concatenated matrix
   * Also, given two corresponding vectors, returns their addition 
   * (think of the vectors as the column l1 norms from the chunks of rows these matrices were derived from in the QR tree reduction)
   */
  private def reduceQR(
      acc: Accumulator[Double],
      a: Tuple2[DenseVector[Double], DenseMatrix[Double]],
      b: Tuple2[DenseVector[Double], DenseMatrix[Double]]): Tuple2[DenseVector[Double], DenseMatrix[Double]] = {
    val begin = System.nanoTime
    val outmat = QRUtils.qrR(DenseMatrix.vertcat(a._2, b._2), false)
    val outcolnorms = a._1 + b._1
    acc += ((System.nanoTime - begin) / 1e6)
    (outcolnorms, outmat)
  }

}

