package com.example.streaming.api

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object UpdateStateByKeyTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Update State By Key Test")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("hdfs://master:9999/streaming/checkpoint") //必须要设置，没设置会报错


    val lines: DStream[String] = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordPairs: DStream[(String, Int)] = words.map(x => (x, 1))

    /**
      * 输入的是(k,v)
      * ("java",1)
      * ("c++",1)
      * ("java",1)
      * 这里给到函数调用的Seq是同一个key的value列表,可以看出updateStateByKey这个函数已经相对相同key的value进行了分组聚合
      * state是对于每个key的state, 这里的state不会超时清除
      */
    //第一个Api
    val resultPairs: DStream[(String, Int)] = wordPairs.updateStateByKey(
      (valueSeq: Seq[Int], keyState: Option[Int]) => {
        println("Seq内容:" + valueSeq + ",State内容:" + keyState)
        Some(keyState.getOrElse(0) + valueSeq.sum)
      }
    )
    resultPairs.print()

    /**
      * 这个重载版本的函数跟上面的最大区别在于，这里是一次过把所有key,Seq 的 Iterator给进去，而不像上面的一次一次的给
      * 返回值也是Iterator,意图明显的告诉我们这是一整批一次过处理，处理完了之后，把整批的结果返回回去，只要(key,Seq)就行，state对外来说已经不关心,最终结果到DStream去
      */
    //第二个Api
    val resultPairs2: DStream[(String, Int)] = wordPairs.updateStateByKey[Int](
      (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
        val list = ListBuffer[(String, Int)]()
        while (iter.hasNext) {
          val (key, valueSeq, keyState) = iter.next //这里是每个key和key对应的value列表和key对应的状态state

          val state = Some(keyState.getOrElse(0) + valueSeq.sum)

          val value = state.getOrElse(0)
          if (key.contains("error")) {
            list += ((key, value)) // Add only keys with contains error
          }
        }
        list.toIterator
      },
      new HashPartitioner(4),
      true
    )
    resultPairs2.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
