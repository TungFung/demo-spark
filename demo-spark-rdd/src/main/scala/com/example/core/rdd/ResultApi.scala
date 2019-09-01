package com.example.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中对结果进行处理的API
 * collect -- Return an array that contains all of the elements in this RDD.
 * toLocalIterator -- Return an iterator that contains all of the elements in this RDD.
 * take -- Take the first num elements of the RDD.
 * first -- Return the first element in this RDD.
 * takeOrdered -- Returns the first k (smallest) elements from this RDD as defined by the specified Ordering.
 * top -- Returns the top k (largest) elements from this RDD as defined by the specified Ordering.
 * foreach
 * foreachPartition
 * isEmpty
 */
object ResultApi {

  /**
   * Returns the first k (smallest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of [[top]].
   * For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).takeOrdered(1)
   *   // returns Array(2)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).takeOrdered(2)
   *   // returns Array(2, 3)
   * }}}
   */
  def testTakeOrdered(sc: SparkContext): Unit = {
    //取一个序列中最小的前几个
  }

  /**
   * Returns the top k (largest) elements from this RDD as defined by the specified
   * implicit Ordering[T] and maintains the ordering. This does the opposite of
   * [[takeOrdered]]. For example:
   * {{{
   *   sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1)
   *   // returns Array(12)
   *
   *   sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)
   *   // returns Array(6, 5)
   * }}}
   */
  def testTop(sc: SparkContext): Unit = {
    //取一个序列中最大的前几个
  }

  /**
   * take -- 取出RDD中的前N条
   * @param sc
   */
  def testTake(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String)] = sc.makeRDD(Seq("Chinese","Math","English"))
    inputRDD.take(2).foreach(println)
    /*
      Chinese
      Math
     */
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testTake(sc)
    sc.stop()
  }
}
