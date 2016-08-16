package org.apache.spark.mllib

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, diag, norm}
import org.apache.spark.mllib.optimization.NNLS
import math.sqrt

object xray {

  def findWeights(X : BDM[Double], XA : BDM[Double]) : BDM[Double] = {
    val H = BDM.zeros[Double](XA.cols, X.cols)
    val ws = NNLS.createWorkspace(XA.cols)
    val ata = (XA.t*XA).toArray

    for(colidx <- 0 until X.cols) {
      val atb = (XA.t*X(::,colidx)).toArray
      val h = NNLS.solve(ata, atb, ws) 
      H(::, colidx) := BDV(h)
    }
    H
  }

  def computeXray( X : BDM[Double], r : Int) : (Array[Int], BDM[Double]) = {

    val C = X.t*X
    val selColIndices = Array.fill[Int](r)(-1)

    def posPartNorm(x : BDV[Double]) : Double = {
      norm(x.map( x => if (x >= 0) x else 0 ))
    }

    // select the first column
    val objVals = (0 until X.cols).map( j =>  posPartNorm(C(::, j)) / sqrt(C(j, j)) ).toList
    selColIndices(0) = objVals.indexOf(objVals.max) 
    var curXA = X(::, selColIndices(0)).asDenseMatrix.t
    var curH = findWeights(X, curXA)

    // select remaining columns
    for (curiter <- 1 until r) {
      val potentialCols = (0 until X.cols).toArray.filter( ! (x => selColIndices contains x)(_) ) 
      val objVals = potentialCols.map( j => posPartNorm( C(::, j) - curH.t*(curXA.t*X(::, j)) ) / sqrt(C(j, j)) )
      selColIndices(curiter) = potentialCols(objVals.zipWithIndex.sortBy(_._1).last._2)

      curXA = BDM.zeros[Double](X.rows, curiter + 1)
      for(colidx <- 0 to curiter) {
        curXA(::, colidx) := X(::, selColIndices(colidx))
      }
      curH = findWeights(X, curXA)
    }

    return (selColIndices, curH)
  }

}
