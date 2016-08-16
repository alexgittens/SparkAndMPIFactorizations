package org.apache.spark.mllib.linalg.distributed
import breeze.linalg.{DenseMatrix, DenseVector}
import java.io.{DataInputStream, FileInputStream, FileWriter, File}

object ConvertDump { 

  type DM = DenseMatrix[Double]
  type DDV = DenseVector[Double]
  type DIV = DenseVector[Int]

  def loadDoubleVector( inf: DataInputStream) : DDV = {
    val len = inf.readInt()
    val v = DenseVector.zeros[Double](len)
    for (i <- 0 until len) {
      v(i) = inf.readDouble()
    }
    v
  }
  
  def loadIntVector( inf: DataInputStream) : DIV = {
    val len = inf.readInt()
    val v = DenseVector.zeros[Int](len)
    for (i <- 0 until len) {
      v(i) = inf.readInt()
    }
    v
  }

  def loadMatrix( inf: DataInputStream) : DM = {
    val (r,c) = Tuple2(inf.readInt(), inf.readInt())
    val m = DenseMatrix.zeros[Double](r,c)
    for (i <- 0 until r; j <- 0 until c) {
      m(i,j) = inf.readDouble()
    }
    m 
  }

  def loadDump(infname: String) : Tuple4[DM, DM, DDV, DDV] = {

    val inf = new DataInputStream( new FileInputStream(infname))

    val eofsU = loadMatrix(inf)
    val eofsV = loadMatrix(inf)
    val evals = loadDoubleVector(inf)
    val mean = loadDoubleVector(inf)

    inf.close()
    (eofsU, eofsV, evals, mean)
  }

  def writeDoubleMatrix(mat: DM, fn: String) = {
    val writer = new FileWriter(new File(fn))
    writer.write("%%MatrixMarket matrix coordinate real general\n")
    writer.write(s"${mat.rows} ${mat.cols} ${mat.rows*mat.cols}\n")
    for(i <- 0 until mat.rows) {
      for(j <- 0 until mat.cols) {
        writer.write(f"${i+1} ${j+1} ${mat(i, j)}%f\n")
      }
    }
    writer.close
  }

  def writeIntVector(vec: DIV, fn: String) = {
    val mat = vec.asDenseMatrix
    val writer = new FileWriter(new File(fn))
    writer.write("%%MatrixMarket matrix coordinate real general\n")
    writer.write(s"${mat.rows} ${mat.cols} ${mat.rows*mat.cols}\n")
    for(i <- 0 until mat.rows) {
      for(j <- 0 until mat.cols) {
        writer.write(s"${i+1} ${j+1} ${mat(i, j)}\n")
      }
    }
    writer.close
  }

  def main(args: Array[String]) {
    val (eofsU, eofsV, eofsS, mean) = loadDump(args(0))
    writeDoubleMatrix(eofsU, s"${args(1)}/colEOFs")
    writeDoubleMatrix(eofsV, s"${args(1)}/rowEOFs")
    writeDoubleMatrix(eofsS.asDenseMatrix, s"${args(1)}/evalEOFs")
    writeDoubleMatrix(mean.asDenseMatrix, s"${args(1)}/rowMeans")
  }
}
