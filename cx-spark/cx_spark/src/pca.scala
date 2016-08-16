import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

object PcaApp  {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PCA")
    val sc = new SparkContext(conf)
    val inpath = "/something"
    val nonzeros = sc.textFile(inpath).map(_.split(",")).map(x => new MatrixEntry(x(0).toLong, x(1).toLong, x(2).toFloat))
    val mat = new CoordinateMatrix(nonzeros).toRowMatrix
    val pc = mat.computePrincipalComponents(10)
    println(pc)
  }
}
