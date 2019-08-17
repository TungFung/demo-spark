package com.example.sparksession

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object RunDemo {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Demo")
      .getOrCreate()

    //session.createDataFrame()
  }

}
