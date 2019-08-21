package com.example.sql

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 写是通过Dataset，读是通过SparkSession
  * spark.write.save
  * write
  * writeStream
  * save
  * saveAsTable
  */
object WriteApi {

  def testWrite(spark: SparkSession): Unit = {
    val fs: FileSystem = FileSystem.get(new Configuration())
    val path: Path = new Path("spark-sql/src/main/resources/output")
    if(fs.exists(path)) fs.delete(path, true)

    val df: DataFrame = spark.createDataFrame(Seq("Chinese" -> 48, "English" -> 98))
    df.write.save("spark-sql/src/main/resources/output/default")
    //会生成一个default目录，里面是数据文件，格式默认是parquet

    df.write.json("spark-sql/src/main/resources/output/json")
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Demo")
      .getOrCreate()
    testWrite(spark)
    spark.stop()
  }
}
