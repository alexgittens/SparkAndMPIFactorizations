package org.apache.spark.climate
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.linalg.{Vector, DenseVector, SparseVector}
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.sql.{SQLContext, Row => SQLRow}
import java.util.Arrays
import java.util.Calendar
import java.text.SimpleDateFormat

object ParquetCombiner {

  def report(message: String, verbose: Boolean = true) = {
    val now = Calendar.getInstance().getTime()
    val formatter = new SimpleDateFormat("H:m:s")

    if(verbose) {
      println("STATUS REPORT (" + formatter.format(now) + "): " + message)
    }
  }

  def main(args: Array[String]) = {
    val conf= new SparkConf().setAppName("CSV to Parquet convertor").
                set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    // load each of the pieces and join them

    val sqlctx = new SQLContext(sc)
    import sqlctx.implicits._

    val outpath = args(0)
    val colarray = new Array[RDD[(Long, DenseVector)]](5)

    for (idx <- 0 to 4) {
      colarray(idx) = {
        sqlctx.parquetFile(outpath + "/mat.parquet" + idx.toString).rdd.map {
          case SQLRow(index: Long, vector: DenseVector) => (index, vector)
        }
      }
    }

    val cols0and1 = colarray(0).join(colarray(1)).map(x => (x._1, new DenseVector(x._2._1.toArray ++ x._2._2.toArray)))
    val cols2and3 = colarray(2).join(colarray(3)).map(x => (x._1, new DenseVector(x._2._1.toArray ++ x._2._2.toArray)))
    val cols0through3 = cols0and1.join(cols2and3).map(x => (x._1, new DenseVector(x._2._1.toArray ++ x._2._2.toArray)))
    val allcols = cols0through3.join(colarray(4)).map(x => (x._1, new DenseVector(x._2._1.toArray ++ x._2._2.toArray)))
    
    val finalcols = allcols.map(x => new IndexedRow(x._1, x._2)).toDF
    finalcols.saveAsParquetFile(outpath + "/finalmat.parquet")
  }
}
