package com.example.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中的持久化操作的一些API
 * persist
 * cache
 * unpersist
 * checkpoint -- 将RDD保存到HDFS中，切断RDD之前的依赖关系
 * localCheckpoint -- 将RDD保存在executor的本地磁盘或内存中，本质上是调用persist(MEM_AND_DISK)
 */
object PersistApi {

  /**
   * Mark this RDD for checkpointing. It will be saved to a file inside the checkpoint
   * directory set with `SparkContext#setCheckpointDir` and all references to its parent
   * RDDs will be removed. This function must be called before any job has been
   * executed on this RDD. It is strongly recommended that this RDD is persisted in
   * memory, otherwise saving it on a file will require recomputation.
   */
  def testCheckpoint(sc: SparkContext): Unit = {
    sc.setCheckpointDir("spark-core/src/main/resources/checkpoint")
    val inputRDD: RDD[String] = sc.textFile("spark-core/src/main/resources/input/wordcount/")
    val flatMapRDD: RDD[String] = inputRDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatMapRDD.map(x => (x, 1))
    val resultRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    resultRDD.checkpoint()
    resultRDD.collect //触发action
    println(resultRDD.toDebugString)

    val result2RDD = resultRDD.zipWithUniqueId()
    result2RDD.collect
    println(result2RDD.toDebugString)
/*
-- 没设置
(2) MapPartitionsRDD[5] at zipWithUniqueId at PersistApi.scala:33 []
 |  ShuffledRDD[4] at reduceByKey at PersistApi.scala:28 []
 +-(2) MapPartitionsRDD[3] at map at PersistApi.scala:27 []
    |  MapPartitionsRDD[2] at flatMap at PersistApi.scala:26 []
    |  spark-core/src/main/resources/input/wordcount/ MapPartitionsRDD[1] at textFile at PersistApi.scala:25 []
    |  spark-core/src/main/resources/input/wordcount/ HadoopRDD[0] at textFile at PersistApi.scala:25 []

-- 设置 checkpoint
(2) MapPartitionsRDD[5] at zipWithUniqueId at PersistApi.scala:33 []
 |  ShuffledRDD[4] at reduceByKey at PersistApi.scala:28 []
 |  ReliableCheckpointRDD[6] at collect at PersistApi.scala:34 []

-- 设置 localCheckpoint
(2) MapPartitionsRDD[6] at zipWithUniqueId at PersistApi.scala:33 []
 |  ShuffledRDD[4] at reduceByKey at PersistApi.scala:28 []
 |      CachedPartitions: 2; MemorySize: 408.0 B; ExternalBlockStoreSize: 0.0 B; DiskSize: 0.0 B
 |  LocalCheckpointRDD[5] at collect at PersistApi.scala:30 []
*/
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testCheckpoint(sc)
    sc.stop()
  }
}
