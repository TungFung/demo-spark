package com.example.streaming.flume

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 通过telnet发数据到flume的netcat sources,经过memory channel给到 avro sink
  * avro sink 是avro socket 管道，直接透传数据，需要spark-streaming先启动，flume后启动，两边才能正常建立tcp连接。
  *
  * flume配置
  * sources -- netcatSrc (192.168.184.130 44445) flume启动一个netcat master server 监听 44445端口的 数据
  *           通过telnet master 44445 给数据到 netcat, flume就能取到netcat的数据
  * channel -- memoryChannel
  * sinks -- arvoSink (192.168.1.7 44446) spark-streaming启动一个avro socket server 44446端口 接收数据
  */
object FlumePushTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Flume Push Word Count")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(3))

    //这里在本地调试，但是不能写localhost, 暴露本机的44446端口作为接收数据端口, 先启动spark程序，后启动flume
    val flumeEventDStream: DStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc, "192.168.1.7", 44446, StorageLevel.MEMORY_AND_DISK_SER_2)

    flumeEventDStream.count().map(event => "Received " + event + " flume events." ).print()

    ssc.start()

    ssc.awaitTermination()
  }

}
