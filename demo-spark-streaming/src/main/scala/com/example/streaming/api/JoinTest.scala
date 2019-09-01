package com.example.streaming.api

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object JoinTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Socket Stream Word Count")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    val lines1: DStream[String] = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs1: DStream[(String, String)] = lines1.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

    lines1.print()


    val lines2: DStream[String] = ssc.socketTextStream("master", 9997, StorageLevel.MEMORY_AND_DISK_SER)
    val kvs2: DStream[(String, String)] = lines2.map { line =>
      val arr = line.split(" ")
      (arr(0), arr(1))
    }

    lines2.print()

    //下面的join操作都是根据key来join的，key相同才能join一起
//    kvs1.join(kvs2).print()
    kvs1.fullOuterJoin(kvs2).print() //ni hao | ni buhao |  (ni,(Some(buhao),Some(hao))) 跟RDD的join操作是一样的
//    kvs1.leftOuterJoin(kvs2).print()
//    kvs1.rightOuterJoin(kvs2).print()

    ssc.start()

    ssc.awaitTermination()
  }

}
