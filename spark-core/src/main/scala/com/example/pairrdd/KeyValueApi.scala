package com.example.pairrdd

import scala.collection.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * keys
 * values
 * lookup
 * collectAsMap
 * partitionBy
 */
object KeyValueApi {

  def testKeyValues(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "B" -> 200, "C" -> 300))
    inputRDD.keys.collect.foreach(println)
    /*
      A
      B
      C
     */
    inputRDD.values.collect.foreach(println)
    /*
      100
      200
      300
     */
  }

  def testLookup(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    inputRDD.lookup("A").foreach(println)
    /*
      100
      200
     */
  }

  def testCollectAsMap(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val map: Map[String, Int] = inputRDD.collectAsMap() // K-V RDD 转Map
  }

  def testPartitionBy(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val resultRDD: RDD[(String, Int)] = inputRDD.partitionBy(new HashPartitioner(2))
    println(resultRDD.partitions.length)//2
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testPartitionBy(sc)
    sc.stop()
  }
}
