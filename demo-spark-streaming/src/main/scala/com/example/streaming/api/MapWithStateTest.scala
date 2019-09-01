package com.example.streaming.api

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.MapWithStateDStream
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext, Time}

/**
  * MapWithState的作用是，
  * 在流式处理中，通常是一次处理完就忘了上一次处理的结果的，
  * 这种withState的操作就可以用来记录状态（相当于有地方作为全局变量）.
  * 利用该特性来做一些相关的操作.
  * currentState: State[Long] 表示状态
  * currentState.getOption.getOrElse(0L) 读取状态
  * currentState.update(sum) 更新状态
  * currentState.isTimingOut() 状态是否超时了，超时了会被移除该状态
  */
object MapWithStateTest {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Map With State Test")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    ssc.checkpoint("hdfs://master:9999/streaming/checkpoint") //必须要设置，没设置会报错


    /**
      * 输入的是(k,v)
      * ("java",1)
      * ("c++",1)
      * ("java",1)
      * 这里相同key给到对应key的state,一开始state没值肯定是空
      */
    // currentBatchTime : 表示当前的Batch的时间
    // key: 表示需要更新状态的key
    // value: 表示当前batch的对应的key的对应的值
    // keyState: key对应的state状态
    val stateSpec: StateSpec[String, Int, Long, (String, Long)] = StateSpec.function((currentBatchTime: Time, key: String, value: Option[Int], keyState: State[Long]) => {
      val sum = value.getOrElse(0).toLong + keyState.getOption.getOrElse(0L)
      val output = (key, sum)
      if (!keyState.isTimingOut()) {
        keyState.update(sum)
      }
      Some(output) //返回的类型就是MapperType
    })

    //timeout: 当一个key超过这个时间没有接收到数据的时候，这个key以及对应的状态会被移除掉
    val initialRDD = sc.parallelize(List(("dummy", 100L), ("source", 32L))) //初始化的state值
    stateSpec.initialState(initialRDD).numPartitions(2).timeout(Seconds(30))

    val lines = ssc.socketTextStream("master", 9998, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" "))
    val wordsDStream = words.map(x => (x, 1))
    val result: MapWithStateDStream[String, Int, Long, (String, Long)] = wordsDStream.mapWithState(stateSpec)

    //result.print() 打印跟这一批次出现关联的

    result.stateSnapshots().print() //打印所有的state

    ssc.start()

    ssc.awaitTermination()
  }

}
