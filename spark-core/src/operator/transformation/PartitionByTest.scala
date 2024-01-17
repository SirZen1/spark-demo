package operator.transformation

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PartitionByTest {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val rdd: RDD[(Int, String)] =
      sparkContext.makeRDD(Array((1, "aaa"), (2, "bbb"), (3, "ccc")), 3)
    val rdd2 = rdd.partitionBy(new HashPartitioner(2))
    rdd2.saveAsTextFile("output/partitionBy")
  }
}
