package com.example.rdd

import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中的一些分组聚合操作
 * groupBy -- 给定一个函数，将RDD中的每行数据映射一个KEY，根据这个KEY做groupBy, 分组中元素的顺序是无序的
 * sortBy -- 对每行RDD数据映射一个值，根据这个值做排序
 * distinct
 * count -- Return the number of elements in the RDD.
 * countApprox -- Approximate(近视值) version of count()
 * countByValue -- Return the count of each unique value in this RDD as a local map of (value, count) pairs.
 * countByValueApprox -- Approximate(近视值) version of countByValue()
 * countApproxDistinct -- Return approximate number of distinct elements in the RDD.
 * min
 * max
 * aggregate -- Aggregate the elements of each partition, and then the results for all the partitions
 * treeAggregate -- Aggregates the elements of this RDD in a multi-level tree pattern.
 */
object AggregateApi {

  /**
   * 给定一个函数，将RDD中的每行数据映射一个KEY，根据这个KEY做groupBy, 分组中元素的顺序是无序的
   */
  def testGroupBy(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String)] = sc.makeRDD(Seq("Chinese","Math","Chinese"))
    val resultRDD: RDD[(Long, Iterable[String])] = inputRDD.groupBy(line => line.hashCode % 3)
    resultRDD.collect.foreach(println)
    /*
      (-2,CompactBuffer(Chinese, Chinese))
      (1,CompactBuffer(Math))
     */
  }

  /**
   * 对每行RDD数据映射一个值，根据这个值做排序
   */
  def testSortBy(sc: SparkContext): Unit = {
    val inputRDD: RDD[String] = sc.makeRDD(Seq("Chinese","Math","English"))
    val resultRDD: RDD[String] = inputRDD.sortBy(line => line.charAt(0), ascending = true)
    resultRDD.collect.foreach(println)
    /*
      Chinese
      English
      Math
     */
  }

  /**
   * 对RDD中重复的行去重
   */
  def testDistinct(sc: SparkContext): Unit = {
    val inputRDD: RDD[String] = sc.makeRDD(Seq("Chinese","Math","Chinese"))
    val resultRDD: RDD[String] = inputRDD.distinct()
    resultRDD.collect.foreach(println)
  }

  def testMaxMin(sc: SparkContext): Unit = {
    val inputRDD: RDD[String] = sc.makeRDD(Seq("Apple","Boy","Chicken"))
    val maxLine: String =  inputRDD.min()
    val minLine: String =  inputRDD.max()
    println("max:" + maxLine + ", min:" + minLine) //max:Apple, min:Chicken
    //这种max min 对字符串比大小看起来没什么意思，具体比较算法要看底层实现才知道，一般不会这么用
    // 用在数据是数值类型的才有意义

    val inputRDD2: RDD[Int] = sc.makeRDD(Seq(100, 300, 200))
    println("max:" + inputRDD2.max() + ", min:" + inputRDD2.min()) //max:300, min:100
  }

  /**
   * Approximate version of count() that returns a potentially incomplete result
   * within a timeout, even if not all tasks have finished.
   *
   * The confidence is the probability that the error bounds of the result will
   * contain the true value. That is, if countApprox were called repeatedly
   * with confidence 0.9, we would expect 90% of the results to contain the
   * true count. The confidence must be in the range [0,1] or an exception will
   * be thrown.
   */
  def testCountApprox(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String)] = sc.makeRDD(Seq("Chinese","Math","English"))
    val result: PartialResult[BoundedDouble] = inputRDD.countApprox(1000L, 0.98)
    println("count approximate:" + result) //count approximate:(final: [3.000, 3.000])
    println("count:" + inputRDD.count()) //count:3
  }

  /**
   * 计算RDD中相同行的数量
   */
  def testCountByValue(sc: SparkContext): Unit = {
    val inputRDD: RDD[(String)] = sc.makeRDD(Seq("Chinese","Math","Chinese"))
    println(inputRDD.countByValue()) //Map(Math -> 1, Chinese -> 2)
  }

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using
   * given combine functions and a neutral "zero value". This function can return a different result
   * type, U, than the type of this RDD, T. Thus, we need one operation for merging a T into an U
   * and one operation for merging two U's, as in scala.TraversableOnce. Both of these functions are
   * allowed to modify and return their first argument instead of creating a new U to avoid memory
   * allocation.
   *
   * zeroValue -- the initial value for the accumulated result of each partition for the
   *                  `seqOp` operator, and also the initial value for the combine results from
   *                  different partitions for the `combOp` operator - this will typically be the
   *                  neutral element (e.g. `Nil` for list concatenation or `0` for summation)
   * seqOp -- an operator used to accumulate results within a partition
   * combOp -- an associative operator used to combine results from different partitions
   */
  def testAggregate(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300))
    inputRDD.persist()

    val zeroValue: Int = 1;
    val eachPartitionAccumulateFunc = (firstValue: Int, nextValue: Int) => {firstValue + nextValue}
    val finalCombineFunc = (firstValue: Int, nextValue: Int) => {firstValue + nextValue}
    val result: Int = inputRDD.aggregate(zeroValue)(eachPartitionAccumulateFunc, finalCombineFunc)

    println("aggregate result:" + result) //aggregate result:605
    //这里为什么是5呢？因为并行度设置为4，就有4个partition,每个partition的初始值是1,就有4.
    // 到了所有分区再合并的时候，又有一个1作为初始值，所以就是5
  }

  /**
   * treeAggregate与aggregate的不同在与，treeAggregate会在分区内对计算的结果进行reduceByKey汇总后，
   * 再给到driver端做reduce进行总和并。depth深度的意思是：对分区划分小组进行reduceByKey后，再对剩下的
   * 进行reduceByKey,reduceByKey需要多少层就由这个depth决定。
   *
   * 所以对比得出，aggregate会有潜在的OOM风险，因为数据都拉回driver端进行总合并，但treeAggregate已经提前
   * 做过一次汇总减少数据量了，所以到了driver端的数据会少很多。
   */
  def testTreeAggregate(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300))
    inputRDD.persist()

    val zeroValue: Int = 1;
    val depth: Int = 2;
    val eachPartitionAccumulateFunc = (firstValue: Int, nextValue: Int) => {firstValue + nextValue}
    val finalCombineFunc = (firstValue: Int, nextValue: Int) => {firstValue + nextValue}
    val result: Int = inputRDD.treeAggregate(zeroValue)(eachPartitionAccumulateFunc, finalCombineFunc, depth)
    println("aggregate result:" + result) //aggregate result:604
    //这里并行度设置为4，所以有4个partition，treeAggregate只在各自分区计算的时候用到初始值，最后的combine不用
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testTreeAggregate(sc)
    sc.stop()
  }

}
