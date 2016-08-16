package org.apache.spark.mllib.linalg.distributed
import breeze.linalg.{DenseMatrix, DenseVector}
import java.io.{DataInputStream, FileInputStream, FileWriter, File}

object ConvertDump { 

  type DM = DenseMatrix[Double]
  type DV = DenseVector[Double]

  def loadVector( inf: DataInputStream) : DV = {
    val len = inf.readInt()
    val v = DenseVector.zeros[Double](len)
    for (i <- 0 until len) {
      v(i) = inf.readDouble()
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

  def loadDump(infname: String) : Tuple11[Double, Double, Double, Double,
  DV, DV, DM, DV, DM, DM, DM]= {

    val inf = new DataInputStream( new FileInputStream(infname))

    //discard parameter information
    val nrows:  Int = inf.readInt()
    val ncols:  Int = inf.readInt()
    val rank:   Int = inf.readInt()
    val slack:  Int = inf.readInt()
    val niters: Int = inf.readInt()
    val nparts: Int = inf.readInt()
    
    val frobnorm = inf.readDouble()
    val rpca_froberr = inf.readDouble()
    val pca_froberr = inf.readDouble()
    val cx_froberr = inf.readDouble()
    val mean = loadVector(inf)
    val rpcaEvals = loadVector(inf)
    val rpcaU = loadMatrix(inf)
    val pcaEvals = loadVector(inf)
    val pcaU = loadMatrix(inf)
    val C = loadMatrix(inf)
    val X = loadMatrix(inf)
    inf.close()

    (frobnorm, rpca_froberr, pca_froberr, cx_froberr, mean, 
      rpcaEvals, rpcaU, pcaEvals, pcaU, C, X)
  }

  def writeMatrix(mat: DM, fn: String) = {
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

  def main(args: Array[String]) {
    val(frobnorm, rpca_froberr, pca_froberr, cx_froberr,
      mean, rpcaEvals, rpcaU, pcaEvals, pcaU, c, x) = loadDump(args(0))
    writeMatrix(DenseVector[Double](frobnorm, rpca_froberr, pca_froberr).asDenseMatrix, s"${args(1)}/computedFrobNorms")
    writeMatrix(mean.asDenseMatrix, s"${args(1)}/computedMean")
    writeMatrix(rpcaU, s"${args(1)}/rpcaU")
    writeMatrix(rpcaEvals.asDenseMatrix, s"${args(1)}/pcaEvals")
    writeMatrix(pcaU, s"${args(1)}/pcaU")
    writeMatrix(pcaEvals.asDenseMatrix, s"${args(1)}/pcaEvals")
    writeMatrix(c, s"${args(1)}/cMat")
    writeMatrix(x, s"${args(1)}/xMat")
  }
}
