package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

object IntersectionTest {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val rdd1 = sparkContext.makeRDD(
      List(1, 2, 3), 2
    )
    val rdd2 = sparkContext.makeRDD(
      List((3,"3"), ("4",4,"a"), (5,"5"), (6,"6")), 2
    )
    val rdd3 = rdd1.zip(rdd2)
    rdd3.saveAsTextFile("output/zip")
  }
}
