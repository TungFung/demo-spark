package com.example.sparkcontext

import org.apache.spark.{SparkConf, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

object RunJob {

  def testRunJob(sc: SparkContext): Unit = {
    val inputRDD: RDD[Int] = sc.makeRDD(Seq(100, 200, 300))
    val partitionProcessFunc = (taskContext: TaskContext, iterator: Iterator[Int]) => {
      println("stageId:" + taskContext.stageId() + ", partitionId:" + taskContext.partitionId() + ", iter:" + iterator)
      iterator.reduceLeft(_ + _)
      /*
        stageId:0, partitionId:0, iter:non-empty iterator //分区里的数据为100
        stageId:0, partitionId:1, iter:non-empty iterator //分区里的数据为200, 300
       */
    }
    val resultHandleFunc = (partitionId: Int, partitionResult: Int) => {
      println("partitionId:" + partitionId + ", partitionResult:" + partitionResult)
      /*
        partitionId:0, partitionResult:100
        partitionId:1, partitionResult:500
       */
    }
    sc.runJob(inputRDD, partitionProcessFunc, resultHandleFunc)
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("Demo")
    val sc: SparkContext = new SparkContext(conf)
    testRunJob(sc)
    sc.stop()
  }

}
