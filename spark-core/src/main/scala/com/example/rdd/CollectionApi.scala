package com.example.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中的一些集合操作API
 * zip -- 左表拼右表
 * zipPartitions -- 左表与右表的分区之间进行给定函数的处理
 * zipWithIndex -- ！！这是单个RDD的操作！！给RDD中的每行加上索引号RDD[(String, Long)]
 * zipWithUniqueId --  ！！这是单个RDD的操作！！给RDD中每行数据，设置个唯一ID，但这个ID顺序不一定是递增的
 * union -- 上表拼下表(并集)
 * ++ -- 就是union
 * intersection -- 交集
 * subtract -- 减集
 * cartesian -- 笛卡尔积
 */
object CollectionApi {

  /**
   * 左右两边打横拼起来
   */
  def testZip(sc: SparkContext): Unit = {
    val leftRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "English", "Math"))
    val rightRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300))
    val resultRDD: RDD[(String, Int)] = leftRDD.zip(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      (Chinese,100)
      (English,200)
      (Math,300)
     */
  }

  /**
   * 要求关联的两边的RDD具有相同的分区数
   * 左右两边都有两个分区，打横着对应
   * left    right
   * lp1      rp1   --> 应用给定的zipFunc对其进行处理
   * lp2      rp2   --> 应用给定的zipFunc对其进行处理
   */
  def testZipPartitions(sc: SparkContext): Unit = {
    val leftRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "English", "Math"), numSlices = 2)
    val rightRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300), numSlices = 2)
    val zipFunc = (iter1: Iterator[String], iter2: Iterator[Int]) => {
      //iter1.zip(iter2) -- 这样就变成了普通zip的效果了
      Iterator("Special" -> 999)
      /*
        (Special,999)
        (Special,999)
       */
    }
    val resultRDD: RDD[(String, Int)] = leftRDD.zipPartitions(rightRDD)(zipFunc)
    resultRDD.collect.foreach(println)
  }

  /**
   * 给rdd中的每一行加上索引号
   */
  def testZipWithIndex(sc: SparkContext): Unit = {
    val leftRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "English", "Math"))
    val resultRDD: RDD[(String, Long)] = leftRDD.zipWithIndex()
    resultRDD.collect.foreach(println)
    /*
      (Chinese,0)
      (English,1)
      (Math,2)
     */
  }

  /**
   * 给RDD中每行数据，设置个唯一ID，但这个ID顺序不一定是递增的
   */
  def testZipWithUniqueId(sc: SparkContext): Unit = {
    val leftRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "English", "Math"))
    val resultRDD: RDD[(String, Long)] = leftRDD.zipWithUniqueId() //不会触发sparkJob
    resultRDD.collect.foreach(println)
    /*
      (Chinese,0)
      (English,1)
      (Math,3)
     */
  }

  /**
   * 并集，不去重
   */
  def testUnion(sc: SparkContext): Unit = {
    val leftRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "English", "Math"))
    val rightRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "American", "Japan"))
    val resultRDD: RDD[String] = leftRDD.union(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      Chinese
      English
      Math
      Chinese
      American
      Japan
     */
  }

  /**
   * 交集,这里面对结果进行了去重
   * 以左边的为准，右边中含有与左边相同的元素
   */
  def testIntersection(sc: SparkContext): Unit = {
    val leftRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "English", "American"))
    val rightRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "American", "Japan"))
    val resultRDD: RDD[String] = leftRDD.intersection(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      American
      Chinese
     */
  }

  /**
   * 减集
   * 以左边为准，减去与右边相同的元素得到的剩下的
   */
  def testSubtract(sc: SparkContext): Unit = {
    val leftRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "English", "American"))
    val rightRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "American", "Japan"))
    val resultRDD: RDD[String] = leftRDD.subtract(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      English
     */
  }

  /**
   * 笛卡尔积
   */
  def testCartesian(sc: SparkContext): Unit = {
    val leftRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "English", "American"))
    val rightRDD: RDD[String] = sc.makeRDD(Seq("Chinese", "American", "Japan"))
    val resultRDD: RDD[(String, String)] = leftRDD.cartesian(rightRDD)
    resultRDD.collect.foreach(println)
    /*
      (Chinese,Chinese)
      (Chinese,American)
      (Chinese,Japan)
      (English,Chinese)
      (American,Chinese)
      (English,American)
      (English,Japan)
      (American,American)
      (American,Japan)
     */
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testCartesian(sc)
    sc.stop()
  }

}
