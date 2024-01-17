package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}


object GlomTest {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val rdd1 = sparkContext.makeRDD(
      List(1, 2, 3, 4),2
    )
    val rdd2 = rdd1.glom()
    println(rdd1.toDebugString)
    println(rdd2.toDebugString)
    rdd2.collect()
    sparkContext.stop()
  }
}
