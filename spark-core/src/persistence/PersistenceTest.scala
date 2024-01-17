package persistence

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 演示RDD内存持久化,磁盘持久化及checkpoint持久化
 */
object PersistenceTest {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("groupByTest")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setCheckpointDir("cp")
    val rdd = sparkContext.makeRDD(
      List(1, 2, 3, 4)
    )

    val rdd1 = rdd.map(i => {
      if(i==1){
        println("+++++++")
      }
      i * 2
    })
    //内存持久化
//    rdd1.cache()
    //磁盘持久化
//    rdd1.persist(StorageLevel.DISK_ONLY)

    //注意checkpoint自己会触发一次作业,因此他会额外触发一次rdd1
    // 为了避免checkpoint额外触发一次rdd1,cache和checkpoint最好联合使用，这样checkpoint的作业会使用cache缓存，进而不触发rdd1执行
//    rdd1.cache()
    rdd1.checkpoint()
    println(rdd1.toDebugString)
    //持久化以后，rdd2和rdd3执行时，只会触发一次rdd1的执行，输出一次"+++++++".
    //若未持久化，rdd2和rdd3执行均会触发rdd1执行，共计输出两次"+++++++"
    val rdd2 = rdd1.foreach(i=>{
      println(i)
    })
    println(rdd1.toDebugString)
    val rdd3 = rdd1.foreach(
      i => {
        println(i + 1)
      }
    )
    sparkContext.stop()
  }
}
