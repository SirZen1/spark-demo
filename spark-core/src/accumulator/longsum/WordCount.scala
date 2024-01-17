package accumulator.longsum

import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用累加器实现单词统计
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
//    badDemo(sparkContext)
    accDemo(sparkContext)
    sparkContext.stop()
  }

  /**
   * 一个错误案例用于引出累加器
   */
  def badDemo(sparkContext: SparkContext):Unit = {
    val rdd1 = sparkContext.makeRDD(
      List(1, 2, 3, 4)
    )
    var sum = 0
    rdd1.foreach(num=>{
      sum = sum +num
    })
    //输出为0，因为sum并不会在driver以及task等线程之间共享。
    // 此外就算在多个线程共享，因为涉及到多个线程修改同一个变量，会引发线程安全问题。为了解决单一变量全局共享写，引入了累加器
    println(sum)
  }

  /**
   * 累加器Demo
   */
  def accDemo(sparkContext: SparkContext): Unit = {
    val rdd1 = sparkContext.makeRDD(List(1, 2, 3, 4))
    //获取一个long类型的累加器
    val sumAcc = sparkContext.longAccumulator("sum")
    rdd1.foreach(num=>{
      //使用累加器,注意每个task都有自己的累加器私有副本,最终会由driver调用累加器的merge将所有私有副本合并得到最终结果
      sumAcc.add(num)
    })

    //打印累加器的值,结果为10
    println(sumAcc.value)
    sparkContext.stop()
  }
}
