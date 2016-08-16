package org.apache.spark.mllib

import org.scalatest._
import breeze.stats.distributions.Rand
import breeze.linalg._
import org.apache.spark.mllib.util.TestingUtils._

class xRaySuite extends FunSuite {
	test("solver for H is working") {
		val n = 50
		val k = 7
		val ell = 20

		val basis = DenseMatrix.eye[Double](n).apply(::, 0 until k)
		val H = DenseMatrix.horzcat( DenseMatrix.eye[Double](k), DenseMatrix.rand(k, ell-k))
		val A = basis * H
		val solvedH = xray.findWeights(A, basis) 

		assert(norm((solvedH - H).toDenseVector) ~== 0 absTol 1e-10)
	}

	test("returns reasonable column indices") {
		val n = 50
		val k = 7
		val ell = 20

		var basisIndices = Rand.permutation(n).draw.slice(0, k)
		val basis = DenseMatrix.eye[Double](n).apply(::, basisIndices)
		val lincombs = basis * DenseMatrix.rand(k, ell-k)
		val reorder = Rand.permutation(ell).draw
		val A = DenseMatrix.horzcat(basis, lincombs).apply(::, reorder).toDenseMatrix
		basisIndices = (0 until k).map(reorder.indexOf(_))

		val (xrayIndices, h) = xray.computeXray(A, k)
		assert(xrayIndices.length == k)

		val overlap = basisIndices.toSet.intersect(xrayIndices.toSet) 
		info(s"Actual columns: ${basisIndices.mkString(" ")}")
		info(s"Returned columns: ${xrayIndices.mkString(" ")}")
		info(s"Recovered ${overlap.size} of $k basis columns out of $ell overall columns")		
	}	
}
