package operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

object GroupByTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByTest")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(
      List(1, 2, 3, 4), 2
    )

    val rdd1 = rdd.groupBy(num => {
      num % 2
    })

    rdd1.collect().foreach(println)
    sparkContext.stop()
  }

}
