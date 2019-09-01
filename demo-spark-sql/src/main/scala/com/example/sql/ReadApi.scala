package com.example.sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * SparkSession读取外部数据
  * def read: DataFrameReader
  * def readStream: DataStreamReader
  *
  * 核心方法
  * spark.read.format().load
  * option -- 参数
  * options
  * format -- 表示读取的文件的格式:text,parquet,json等
  * text
  * textFile
  * json
  * parquet
  * avro
  * orc
  * table
  * jdbc
  */
object ReadApi {

  def testReadText(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.text("spark-sql/src/main/resources/text.md")
    df.printSchema()
    /*
      root
        |-- value: string (nullable = true)
     */
    df.show()
    /*
      +----------------+
      |           value|
      +----------------+
      |    hello ni hao|
      |wo shi shui shui|
      +----------------+
     */
  }

  def testReadTextFile(spark: SparkSession): Unit = {
    val ds: Dataset[String] = spark.read.textFile("spark-sql/src/main/resources/text.md")
    ds.printSchema()
    ds.show()
  }

  def testReadJson(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/src/main/resources/device_info_json")
    df.printSchema()
    df.show()
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Demo")
      .getOrCreate()
    testReadTextFile(spark)
    spark.stop()
  }
}
