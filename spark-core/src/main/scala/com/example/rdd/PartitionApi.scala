package com.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中对分区的一些操作
 * repartition -- 调用的就是coalesce 并使用shuffle
 * coalesce -- 改变分区数，小变大需要shuffle,大变小如果幅度不是太大不用shuffle
 * glom -- 这个用scala worksheet实验
 */
object PartitionApi {

  /**
   * Return a new RDD that is reduced into `numPartitions` partitions.
   *
   * This results in a narrow dependency, e.g. if you go from 1000 partitions
   * to 100 partitions, there will not be a shuffle, instead each of the 100
   * new partitions will claim 10 of the current partitions. If a larger number
   * of partitions is requested, it will stay at the current number of partitions.
   *
   * However, if you're doing a drastic(激烈的) coalesce, e.g. to numPartitions = 1,
   * this may result in your computation taking place on fewer nodes than
   * you like (e.g. one node in the case of numPartitions = 1). To avoid this,
   * you can pass shuffle = true. This will add a shuffle step, but means the
   * current upstream partitions will be executed in parallel (per whatever
   * the current partitioning is).
   *
   * @note With shuffle = true, you can actually coalesce to a larger number
   * of partitions. This is useful if you have a small number of partitions,
   * say 100, potentially（潜在地） with a few partitions being abnormally large. Calling
   * coalesce(1000, shuffle = true) will result in 1000 partitions with the
   * data distributed using a hash partitioner. The optional partition coalescer
   * passed in must be serializable.
   */
  def testCoalesce(sc: SparkContext): Unit = {
    val inputRDD: RDD[String] = sc.makeRDD(Seq("Chinese","Math","English"), numSlices = 1000)
    println("inputRDD.partitions.length:" + inputRDD.partitions.length) // 2

    val resultRDD: RDD[String] = inputRDD.coalesce(1, false)
    println("resultRDD.partitions.length:" + resultRDD.partitions.length) //10
    println(resultRDD.glom().collect)
    /*
      小变大，不shuffle, 还是小，shuffle后，就能真的变大了
      大变小，不用shuffle也可以，不过如果一下减少太猛烈的话会有性能问题，这时候用shuffle就不会有性能问题了
     */
  }

  def testGlom(sc: SparkContext): Unit = {
    val inputRDD: RDD[String] = sc.makeRDD(Seq("Chinese","Math","English"), numSlices = 10)
    println(inputRDD.glom().collect) //[[Ljava.lang.String;@518cf84a 看不到的这样
    //要想看到不同分区里面的数据是怎样的，用Spark-Shell执行，直接glom.collect就行，返回的变量里能看到
    //res0: Array[Array[String]] = Array(Array(), Array(), Array(), Array(Chinese), Array(), Array(), Array(Math), Array(), Array(), Array(English))
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testCoalesce(sc)
    sc.stop()
  }
}
