package com.example.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * agg
  * rollup
  * cube
  * describe
  * explode
  */
object Api {

  case class TrackerSession(session_id: String, session_server_time: String,
                            cookie: String, cookie_label: String, ip: String, landing_url: String,
                            pageview_count: Int, click_count: Int, domain: String, domain_label: String)

  def testAgg(spark: SparkSession): Unit = {
    import spark.implicits._
    val sessionDS = spark.read.parquet(s"spark-sql/src/main/resources/trackerSession").as[TrackerSession]

    import org.apache.spark.sql.functions._
    val df: DataFrame = sessionDS.groupBy("cookie_label").agg(
      min("pageview_count").as("min_pv_count"),
      max("pageview_count").as("max_pv_count"),
      avg("pageview_count").as("avg_pv_count"),
      count("pageview_count").as("count_pv_count")
    )
    val finalDF: DataFrame = df.withColumn("age_delta", df("max_pv_count") - df("min_pv_count"))
    finalDF.show()
  }

  def testRollup(spark: SparkSession): Unit = {

  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Demo")
      .getOrCreate()
    testAgg(spark)
    spark.stop()
  }
}
