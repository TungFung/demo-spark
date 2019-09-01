package com.example.streaming

import com.example.streaming.receiver.CustomerReceiver
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object ReceiverStreamWordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Socket Stream Word Count")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(1))

    val lines: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("master", 9998))

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordPairs: DStream[(String, Int)] = words.map(e => (e, 1))

    val wordCounts: DStream[(String, Int)] = wordPairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()

    //ssc.stop(false)

    ssc.awaitTermination()
  }

}
