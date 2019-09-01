package com.example.streaming.api

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.Queue

object TransformRDDTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Transform RDD Test")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    val rddQueue = new Queue[RDD[String]]()
    rddQueue += ssc.sparkContext.makeRDD(Seq("Java", "C++", "Java"))

    val inputDStream: DStream[String] = ssc.queueStream(rddQueue)

    //第一种
    val transformDStream: DStream[(String, Int)] = inputDStream.transform(
      (rdd: RDD[String]) => {
        //把DStream转成RDD进行处理,处理完了返回的结果类型还是DStream,但里面的类型可以不一样
        rdd.map(x => (x, 1))
      }
    )
    transformDStream.print()

    //第二种
    //inputDStream foreach成RDD即时没数据，也会转为一个没数据的RDD,因而还是会遍历一次
    inputDStream.foreachRDD(
      (rdd: RDD[String]) => {
        println("每个RDD进行处理") //后面的时间这行也会执行,没内容的rdd这里foreach也会执行一次
        rdd.foreach(println) //这里只是打印
      }
    )

    ssc.start()

    ssc.awaitTermination()
  }

}
