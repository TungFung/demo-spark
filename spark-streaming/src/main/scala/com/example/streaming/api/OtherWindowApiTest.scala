package com.example.streaming.api

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object OtherWindowApiTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Window Api Test")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER_2)

    //每过2秒钟，然后显示统计前10秒的数据量
    lines.countByWindow(Seconds(10), Seconds(2)).print()

    val words = lines.flatMap(_.split(" "))

    //每过2秒钟，然后用reduce func来聚合前10秒的数据
    words.reduceByWindow((a: String, b: String) => a + b, Seconds(10), Seconds(2)).print()

    //每过2秒钟，对前10秒的单词计数，相当于words.map((_, 1L)).reduceByKeyAndWindow(_ + _, _ - _)
    words.countByValueAndWindow(Seconds(10), Seconds(2)).print()

    //启动Streaming处理流
    ssc.start()

    //等待Streaming程序终止
    ssc.awaitTermination()
  }

}
