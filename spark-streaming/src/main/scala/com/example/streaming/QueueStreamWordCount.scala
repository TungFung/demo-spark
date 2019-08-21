package com.example.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Queue

object QueueStreamWordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Queue Stream Word Count")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(1))

    // 创建一个RDD类型的queue
    val rddQueue = new Queue[RDD[Int]]()
    rddQueue += ssc.sparkContext.makeRDD(1 to 1000, 10)

    // 创建QueueInputDStream 且接受数据和处理数据
    val inputStream = ssc.queueStream(rddQueue)

    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    reducedStream.print()

    ssc.start()
    //ssc.stop(false)

    ssc.awaitTermination()
  }

}
