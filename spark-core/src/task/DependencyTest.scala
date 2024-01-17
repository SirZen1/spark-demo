package task

import org.apache.spark.{ShuffleDependency, SparkConf, SparkContext}


/**
 * 测试rdd2在coalesce函数关闭shuffle机制时，是否会产生宽依赖
 */
object DependencyTest{
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
    val rdd = sparkContext.makeRDD(
      List(1, 2, 3, 4), 1
    )
    val rdd2 = rdd.coalesce(2)
    //这个输出的dependency是个动态匿名类看不出是不是宽依赖ShuffleDependency.但结合日志仅有阶段ResultStage可见是窄依赖
    println(rdd2.dependencies)

    val rdd3 = rdd.coalesce(2, true)
    //输出OnetoOneDependency，看似是窄依赖，但是结合日志会产生shufflestage阶段，因此rdd和rdd3直接是宽依赖
    println(rdd3.dependencies)
    rdd2.collect()
    rdd3.collect()
    sparkContext.stop()

  }
}
