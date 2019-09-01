package com.example.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * schema
  * printSchema
  * explain -- Prints the plans (logical and physical) to the console for debugging purposes.
  * dtypes -- Returns all column names and their data types as an array.
  * columns -- Returns all column names as an array.
  * isLocal -- 查看是否能直接在driver端跑
  * isStreaming -- Returns true if this Dataset contains one or more sources that continuously return data as it arrives.
  * show
  * take -- 返回Array[Row]
  * head -- 返回Array[Row]
  * first -- 返回Row
  * inputFiles -- 显示数据来源的文件地址
  */
object DebugApi {

  def testDebug(spark: SparkSession): Unit = {
    val df: DataFrame = spark.createDataFrame(Seq("Chinese" -> 48, "English" -> 98))
    println(df.isLocal) //true
    println(df.isStreaming) //false

    val df2: DataFrame = spark.read.json("spark-sql/src/main/resources/device_info_json")
    df2.inputFiles.foreach(println)
    //file:///C:/Users/Eric/IdeaProjects/demo-spark/spark-sql/src/main/resources/device_info_json

    df2.explain(true)
    /*
      有很多细节的东西这里不显示
      == Parsed Logical Plan ==
      == Analyzed Logical Plan ==
      == Optimized Logical Plan ==
      == Physical Plan ==
     */

    df2.dtypes.foreach(println)
    /*
      (battery_level,LongType)
      (c02_level,LongType)
      (cca3,StringType)
      (cn,StringType)
      (device_id,LongType)
      (device_type,StringType)
      (ip,StringType)
      (signal,LongType)
      (temp,LongType)
      (timestamp,LongType)
     */
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Demo")
      .getOrCreate()
    testDebug(spark)
    spark.stop()
  }
}
