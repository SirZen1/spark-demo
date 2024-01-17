package broadcast

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 演示一下广播变量的使用
 */
object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val sparkConf =
      new SparkConf().setMaster("local[*]").setAppName("spark")
    val sparkContext = new SparkContext(sparkConf)
//    badDemo(sparkContext)
    broadDemo(sparkContext)
    sparkContext.stop()
  }

  /**
   * 一个错误案例用于引出累加器
   */
  def badDemo(sparkContext: SparkContext): Unit = {
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))
    val map = mutable.Map((1, "a"), (2,"b"), (3,"c"), (4,"d"))
    val rdd1 = rdd.map(num => {
      //在本例中,我们直接把map传入算子内部,这会导致spark为每个task都拷贝出一份私有map
      //如果map过大或者task数量过多,都会导致存储空间大量浪费。基于上述原因，我们可以用广播变量
      //广播变量可以使得处于同一Executor的task共享同一变量
      map.getOrElse(num, "null")
    })
    rdd1.collect().foreach(println)
  }

  /**
   * 广播变量示例
   */
  def broadDemo(sparkContext: SparkContext): Unit = {
    val rdd = sparkContext.makeRDD(List(1, 2, 3, 4))
    val map = mutable.Map((1, "a"), (2, "b"), (3, "c"), (4, "d"))
    //设置广播变量
    val broadMap = sparkContext.broadcast(map)
    val rdd1 = rdd.map(num => {
      //取出广播变量
      broadMap.value.getOrElse(num, "null")
    })
    rdd1.collect().foreach(println)
  }
}
