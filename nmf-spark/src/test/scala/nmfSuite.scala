package org.apache.spark.mllib

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import edu.berkeley.cs.amplab.mlmatrix.{RowPartitionedMatrix}

import org.scalatest._
import breeze.linalg._
import breeze.stats.distributions.Rand
import org.apache.spark.mllib.util.TestingUtils._

class nmfSuite extends FunSuite with BeforeAndAfterAll {

	private var sc: SparkContext = null
	private val conf = new SparkConf().setAppName("NMF Test Suite").setMaster("local")

	override protected def beforeAll(): Unit = {
		sc = new SparkContext(conf) 
	}

	override protected def afterAll(): Unit = {
		sc.stop()
	}	

	test("NMF works locally on a separable matrix") {
		// about 800MB in doubles
		val m = 500000
		val k = 7
		val n = 200
		val rowsPerPartition = Array.fill[Int](1000)(500)

		var basisIndices = Rand.permutation(m).draw.slice(0, k)
		val W = DenseMatrix.zeros[Double](m, k)
		(0 until k) foreach { idx => W(basisIndices(idx), idx) = 1.0 }

		val reorder = Rand.permutation(n).draw	
		basisIndices = (0 until k).map( reorder.indexOf(_) )
		val H = DenseMatrix.horzcat( DenseMatrix.eye[Double](k), DenseMatrix.rand(k, n-k)).apply(::, reorder).toDenseMatrix
		val A = W * H

		val chunksRDD = sc.parallelize( (0 to 1000).map(chunkIdx =>
			A(chunkIdx*500 to (chunkIdx+1)*500, ::).toDenseMatrix ))
		val rowPartitionedA = RowPartitionedMatrix.fromMatrix(chunksRDD)
		val (solvedIndices, W, H) = nmf.fullNMF(rowPartitionedA, k)
		
		val overlap = basisIndices.toSet.intersect(solvedIndices.toSet)
		info(s"original basis column indices: ${basisIndices.mkString(" ")}")
		info(s"returned basis column indices: ${solvedIndices.mkString(" ")}")
		info(s"overlapped ${overlap.size} of $k basis indices out of $n choices")

		val relerr = norm((A - W*H).flatten())/norm(A.flatten())
		info(s"relative frob norm err: $relerr")
		assert(relerr < 1)
	}
}
