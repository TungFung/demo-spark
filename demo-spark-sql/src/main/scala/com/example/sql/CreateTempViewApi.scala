package com.example.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * TempView视图的生命周期依赖于SparkSession类
  * createTempView
  * createOrReplaceTempView
  * spark.catalog.dropTempView
  *
  * GlobalTempView视图的生命周期取决于spark application本身
  * createGlobalTempView
  * createOrReplaceGlobalTempView
  * spark.catalog.dropGlobalTempView
  *
  * spark.catalog.listDatabases.show()
  * spark.catalog.listTables.show()
  */
object CreateTempViewApi {

  def testCreateTempView(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/src/main/resources/device_info_json")
    df.createOrReplaceTempView("test_view")

    val sqlDf: DataFrame = spark.sql("SELECT * FROM test_view")
    sqlDf.printSchema()
    sqlDf.show()

    spark.catalog.listDatabases.show()
    /*
      +-------+----------------+--------------------+
      |   name|     description|         locationUri|
      +-------+----------------+--------------------+
      |default|default database|file:/C:/Users/Er...|
      +-------+----------------+--------------------+
     */

    spark.catalog.listTables.show()
    /*
      +---------+--------+-----------+---------+-----------+
      |     name|database|description|tableType|isTemporary|
      +---------+--------+-----------+---------+-----------+
      |test_view|    null|       null|TEMPORARY|       true|
      +---------+--------+-----------+---------+-----------+
     */

    spark.catalog.dropTempView("test_view")
  }

  def testCreateGlobalTempView(spark: SparkSession): Unit = {
    val df: DataFrame = spark.read.json("spark-sql/src/main/resources/device_info_json")
    df.createOrReplaceGlobalTempView("test_view")

    val sqlDf: DataFrame = spark.sql("SELECT * FROM global_temp.test_view")
    sqlDf.printSchema()
    sqlDf.show()

    val sqlDf2: DataFrame = spark.newSession().sql("SELECT * FROM global_temp.test_view")
    sqlDf2.printSchema()
    sqlDf2.show()

    spark.catalog.listDatabases.show()
    /*
      +-------+----------------+--------------------+
      |   name|     description|         locationUri|
      +-------+----------------+--------------------+
      |default|default database|file:/C:/Users/Er...|
      +-------+----------------+--------------------+
     */

    spark.catalog.listTables.show()
    /*
      +----+--------+-----------+---------+-----------+
      |name|database|description|tableType|isTemporary|
      +----+--------+-----------+---------+-----------+
      +----+--------+-----------+---------+-----------+
     */

    spark.catalog.dropGlobalTempView("test_view")
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Demo")
      .getOrCreate()
    testCreateGlobalTempView(spark)
    spark.stop()
  }
}
