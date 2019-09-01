package com.example.core.rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * RDD中的一些Map API
 * map
 * flatMap
 * mapPartitions
 * mapPartitionsWithIndex
 * keyBy
 */
object MapApi {

  /**
   * mapPartitions -- 以partition为维度进行map
   */
  def testMapPartitions(sc: SparkContext): Unit = {
    val inputRDD: RDD[String] = sc.makeRDD(Seq("Chinese","Math","English"))
    val resultRDD: RDD[String] = inputRDD.mapPartitions((partition: Iterator[String]) => {
      partition.map(line => line + "-append")
    })
    resultRDD.collect.foreach(println)
  }

  def testMapPartitionWithIndex(sc: SparkContext): Unit = {
    val inputRDD: RDD[String] = sc.makeRDD(Seq("Chinese","Math","English"))
    val resultRDD: RDD[String] = inputRDD.mapPartitionsWithIndex((index, partition) => {
      println("Index:" + index + "  partition:" + partition)
      /* 这里并行度设置为多少，就有多少partition
        Index:2  partition:non-empty iterator
        Index:0  partition:empty iterator
        Index:3  partition:non-empty iterator
        Index:1  partition:non-empty iterator
       */
      partition
    })
    resultRDD.collect.foreach(println)
  }

  /**
   * keyBy -- map(x => (cleanedF(x), x))
   */
  def testKeyBy(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String)] = sc.makeRDD(Seq("Chinese","Math","English"))
    val resultRDD: RDD[(String, String)] = inputRDD.keyBy(x => x + "-append" )
    resultRDD.collect.foreach(println)
    /*
      (Chinese-append,Chinese)
      (Math-append,Math)
      (English-append,English)
     */
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    //testKeyBy(sc)
    //testMapPartitions(sc)
    testMapPartitionWithIndex(sc)
    sc.stop()
  }
}
