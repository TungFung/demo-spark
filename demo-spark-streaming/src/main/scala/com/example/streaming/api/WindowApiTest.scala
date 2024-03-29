package com.example.streaming.api

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WindowApiTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Window Api Test")

    val sc: SparkContext = new SparkContext(conf)

    //每隔1秒生成1个新的DStream
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER_2)

    //windowDuration表示窗口的宽度, slideDuration表示滑动窗口的幅度
    val windowDStream: DStream[String] = lines.window(Seconds(20), Seconds(10))

    windowDStream.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
