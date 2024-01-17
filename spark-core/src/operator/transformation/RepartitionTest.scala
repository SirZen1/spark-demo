package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

object RepartitionTest {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(
      List(1, 2, 3, 4), 1
    )
    val rdd2 = rdd.coalesce(4)
    rdd2.saveAsTextFile("output/repartition")
  }
}
