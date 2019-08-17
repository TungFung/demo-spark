package com.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中的一些采样本API
 * sample -- Return a sampled subset of this RDD. 每个分区采集一点按照一定算法
 * taskSample -- Return a fixed-size sampled subset of this RDD in an array.
 * randomSplit -- Randomly splits this RDD with the provided weights.
 */
object SampleApi {

  /**
   * 对RDD中的每个分区采样，按照一定的算法
   * withReplacement -- 有没放回的采样算法
   * fraction -- 因子，每个元素被抽取的概率
   * seed -- 种子，用来生成随机数
   */
  def testSample(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300, 289, 409, 500))
    val resultRDD: RDD[Int] = inputRDD.sample(true, 0.5)
    resultRDD.collect.foreach(println) //每次采集出的数据量是不固定的
  }

  /**
   * 从RDD的各个分区中抽取一些数据返回一个Array,抽取出来的数据量是固定的
   */
  def testTaskSample(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300, 289, 409, 500))
    val result: Array[Int] = inputRDD.takeSample(true, 3)
    result.foreach(println) //每次采集出的数据量是固定
  }

  /**
   * 按照给定权重划分成多个RDD，划分出来的这些RDD与原RDD有着相同的分区数
   */
  def testRandomSplit(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300, 289, 409, 500))
    val result: Array[RDD[Int]] = inputRDD.randomSplit(Array(0.2, 0.4, 0.5))
    result.foreach(rdd => {
      println(s"$rdd")
      rdd.collect.foreach(println)}

      /*
        MapPartitionsRDD[1] at randomSplit at SampleApi.scala:40
        MapPartitionsRDD[2] at randomSplit at SampleApi.scala:40
        200
        289
        409
        500
        MapPartitionsRDD[3] at randomSplit at SampleApi.scala:40
        100
        300
       */
    )
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testRandomSplit(sc)
    sc.stop()
  }
}
