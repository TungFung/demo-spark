package com.example.pairrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * cogroup
 * groupWith -- 就是cogroup的别名
 * join
 * leftOuterJoin,
 * rightOuterJoin,
 * fullOuterJoin,
 * subtractByKey
 */
object CollectionApi {

  def testCogroup(sc: SparkContext): Unit = {
    val leftRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val rightRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "C" -> 300))
    val resultRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = leftRDD.cogroup(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      (A,(CompactBuffer(100, 200),CompactBuffer(100)))
      (C,(CompactBuffer(300),CompactBuffer(300)))
     */
  }

  def testJoin(sc: SparkContext): Unit = {
    val leftRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val rightRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "C" -> 300))
    val resultRDD: RDD[(String, (Int, Int))] = leftRDD.join(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      (A,(100,100))
      (A,(200,100))
      (C,(300,300))
     */
  }

  def testLeftOuterJoin(sc: SparkContext): Unit = {
    val leftRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100))
    val rightRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "C" -> 300))
    val resultRDD: RDD[(String, (Int, Option[Int]))] = leftRDD.leftOuterJoin(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      (A,(100,Some(100)))
     */
  }

  def testSubtractByKey(sc: SparkContext): Unit = {
    val leftRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "B" -> 200, "C" -> 300))
    val rightRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "C" -> 300))
    val resultRDD: RDD[(String, Int)] = leftRDD.subtractByKey(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      (B,200)
     */
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testSubtractByKey(sc)
    sc.stop()
  }
}
