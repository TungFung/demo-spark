package com.example.streaming

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object StructuredStreamWordCount {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Structure Stream Word Count")
      .getOrCreate()

    val lines: Dataset[Row] = spark.readStream
      .format("socket")
      .option("host", "master")
      .option("port", 9998)
      .load()

    import spark.implicits._
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))

    val wordCounts: Dataset[Row] = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

}
