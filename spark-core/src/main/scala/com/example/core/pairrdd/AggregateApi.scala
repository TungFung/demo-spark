package com.example.core.pairrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * combineByKeyWithClassTag -- combine是组合的意思，对相同key的应用给定的函数，灵活性很高
 * combineByKey -- 没传ClassTag的combineByKeyWithClassTag，这是旧的API
 * aggregateByKey -- aggregate是合计的意思，对相同key的值在各自分区合计，然后再汇总合计
 * countByKey
 * countByKeyApprox
 * countApproxDistinctByKey
 * groupByKey -- 根据key分组，底层通过cogroup实现
 */
object AggregateApi {

  /**
   * createCombiner -- 首次遇到这个key就用这个函数执行
   * mergeValue -- 再次遇到这个key用这个函数执行，不用createCombiner
   * mergeCombiners -- 合并各个分区的结果
   * map side combine -- 意思是在map端先进行一次combine聚合下结果后再通过网络传输shuffle,减少数据量
   * partitioner -- 对原始数据先进行分区，combineByKey也会用这个分区器且用相同的分区数，这样就不发生shuffle
   */
  def testCombineByKeyWithClassTag(sc: SparkContext): Unit = {
    //统计每个key对应的value的累计值，以及key出现的次数
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
      .partitionBy(new HashPartitioner(2)) //如果已经分区了,并且与combineByKey拥有相同的分区数，就不会发生shuffle
      .persist()
    //createCombiner函数是当key首次出现的时候调用的
    val createCombiner: Int => (Int,Int) = (value: Int) => {(value, 1)} //这里的函数返回值作为下次merge的acc参数
    //mergeValue函数是当key再次出现的时候调用的
    val mergeValue = (acc: (Int,Int), value: Int) => {
      val accValue = acc._1 + value //value值累加
      val keyCount = acc._2 + 1 //key次数累加，统计用
      (accValue, keyCount)
    }
    //mergeCombiners函数是reduce各个分区的结果
    val mergeCombiners = (acc1: (Int,Int), acc2: (Int,Int)) => {
      val accValue = acc1._1 + acc2._1 //各分区的value值累加
      val keyCount = acc1._2 + acc2._2 //各分区的key次数累加
      (accValue, keyCount)
    }
    //val resultRDD: RDD[(String,(Int, Int))] = inputRDD.combineByKey(createCombiner, mergeValue, mergeCombiners, 2)
    val resultRDD: RDD[(String,(Int, Int))] = inputRDD.combineByKeyWithClassTag(createCombiner, mergeValue, mergeCombiners, new HashPartitioner(2))
    resultRDD.collect.foreach(println)
    /*
      (A,(300,2))
      (C,(300,1))
     */
  }

  /**
   * 对相同key的值应用aggregate
   */
  def testAggregateByKey(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
    val zeroValue = 1 //遇到每一个key都会应用这个初始值
    val seqOp = (v1: Int, v2: Int) => {v1 + v2} //对相同key的value在各自分区内执行给定算法
    val combOp = (v1: Int, v2: Int) => {v1 + v2} //对相同key的seqOp后的结果执行给定算法
    val resultRDD: RDD[(String, Int)] = inputRDD.aggregateByKey(zeroValue)(seqOp, combOp)
    resultRDD.collect.foreach(println)
    /*
      (A,302)
      (C,301)
     */
  }

  def testCogroup(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String, Int)] = sc.makeRDD(Seq("A" -> 100, "A" -> 200, "C" -> 300))
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testAggregateByKey(sc)
    sc.stop()
  }
}
