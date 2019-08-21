package com.example.streaming.api

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKeyAndWindowTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Window Api Test")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

    ssc.checkpoint("hdfs://master:9999/streaming/checkpoint") //必须要设置，没设置会报错

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER_2)

    val words = lines.flatMap(_.split(" "))

    val wordPair = words.map(x => (x, 1))

    //其实跟reduceByKey一样，只是数据源是这个窗口内产生的数据,slideDuration的时间必须是batchInterval的整数倍,每过10秒，计算前面15秒内的数据
    val wordCounts = wordPair.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(15), Seconds(10))
    wordCounts.print()

    /**
      * reduce the new values that entered the window (e.g., adding new counts)
      * "inverse reduce" the old values that left the window (e.g., subtracting old counts)
      */
    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //
    val wordCountsOther = wordPair.reduceByKeyAndWindow(
      (a: Int, b: Int) => a + b,//reduceFunc associative and commutative reduce function
      (a: Int, b: Int) => a - b,//invReduceFunc inverse reduce function; such that for all y, invertible x:`invReduceFunc(reduceFunc(x, y), x) = y`
      Seconds(15), Seconds(5))
    wordCountsOther.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
