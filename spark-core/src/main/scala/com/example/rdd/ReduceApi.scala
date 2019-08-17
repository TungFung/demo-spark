package com.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

/**
 * RDD中Reduce相关API
 * reduce
 * treeReduce
 * fold
 */
object ReduceApi {

  /**
   * Reduces the elements of this RDD using the specified commutative(交换的) and
   * associative(结合的) binary operator.
   */
  def testReduce(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300))
    val result: Int = inputRDD.reduce((f1, f2) => { f1 + f2})
    println(result)
  }

  /**
   * 底层调用了treeAggregate
   * Reduces the elements of this RDD in a multi-level tree pattern.
   * depth -- suggested depth of the tree (default: 2)
   */
  def testTreeReduce(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300))
    val result: Int = inputRDD.treeReduce((f1, f2) => { f1 + f2}, depth = 2)
    println(result)
  }

  def testFold(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300))
    val result: Int = inputRDD.fold(1)((f1: Int, f2: Int) => {f1 + f2})
    println(result) //603
    // 这里并行度设置为2，两个分区各自用了一次zeroValue,然后结果会调的时候又用一次zero（对所有分区的回调只应用一次）
    // 所以 2 + 1 =3
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testFold(sc)
    sc.stop()
  }

}
