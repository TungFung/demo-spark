package com.example.streaming.flume

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkSink作为 flume的 sink, 数据直接存储在flume上, spark-streaming去pull
  * 需要上传commons-lang3-3.5.jar、spark-streaming-flume-sink_2.11-2.2.3.jar 到flume的/lib目录下,
  * 同时注意flume内使用的scala版本的jar包不一样的也要替换
  * 这种模式符合at least once 语义
  *
  * flume配置需要引用上传的spark jar包中的 SparkSink
  * agent.sinks.sparkSink.type = org.apache.spark.streaming.flume.sink.SparkSink
  */
object FlumePullTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Flume Poll Word Count")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(3))

    val flumeEventDStream: DStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "192.168.184.130", 44446, StorageLevel.MEMORY_AND_DISK_SER_2)

    flumeEventDStream.count().map(event => "Received " + event + " flume events." ).print()

    ssc.start()

    ssc.awaitTermination()
  }

}
