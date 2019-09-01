package com.example.core.sparkcontext

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)

    val inputRDD: RDD[String] = sc.textFile("spark-core/src/main/resources/input/wordcount/")

    val resultRDD: RDD[(String, Int)] = inputRDD
      .flatMap(_.split(" "))
      .map(x => (x, 1))
      .reduceByKey(_ + _)
    resultRDD.collect.foreach(println)

    sc.stop()
  }

}
