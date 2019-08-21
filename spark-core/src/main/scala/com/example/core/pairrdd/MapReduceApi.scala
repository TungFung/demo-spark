package com.example.core.pairrdd

import scala.collection.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * foldByKey -- 底层调用combineByKeyWithClassTag
 * reduceByKey -- 底层调用combineByKeyWithClassTag
 * reduceByKeyLocally
 * mapValues,
 * flatMapValues
 */
object MapReduceApi {

  /**
   * 相同key的划分为一组，进行累积计算
   */
  def testFoldByKey(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val resultRDD: RDD[(String, Int)] = inputRDD.foldByKey(1)(_ + _)
    resultRDD.collect.foreach(println)
    /*
      (A,302)
      (C,301)
     */
  }

  def testReduceByKey(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val resultRDD: RDD[(String, Int)] = inputRDD.reduceByKey(_ + _)
    resultRDD.collect.foreach(println)
    /*
      (A,300)
      (C,300)
     */
  }

  /**
   * 会先在worker端合并下结果再传输结果shuffle，返回的类型是Map而不是RDD
   */
  def testReduceByKeyLocally(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val resultMap: Map[String, Int] = inputRDD.reduceByKeyLocally(_ + _)
    resultMap.foreach(println)
    /*
      (A,300)
      (C,300)
     */
  }

  def testMapValues(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val resultRDD: RDD[(String, Int)] = inputRDD.mapValues( (value: Int) => {
      value + 1
    })
    resultRDD.collect.foreach(println)
  }

  def testFlatMapValues(sc: SparkContext): Unit = {

  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testMapValues(sc)
    sc.stop()
  }
}
