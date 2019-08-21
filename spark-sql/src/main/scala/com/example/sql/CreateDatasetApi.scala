package com.example.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 创建DataFrame
  * def createDataFrame[A <: Product : TypeTag](rdd: RDD[A]): DataFrame
  * def createDataFrame[A <: Product : TypeTag](data: Seq[A]): DataFrame
  * def createDataFrame(rowRDD: RDD[Row], schema: StructType): DataFrame
  * def createDataFrame(rowRDD: JavaRDD[Row], schema: StructType): DataFrame
  * def createDataFrame(rdd: JavaRDD[_], beanClass: Class[_]): DataFrame
  * def createDataFrame(data: java.util.List[_], beanClass: Class[_]): DataFrame
  * def createDataFrame(rows: java.util.List[Row], schema: StructType): DataFrame
  * def createDataFrame(rdd: RDD[_], beanClass: Class[_]): DataFrame
  * def baseRelationToDataFrame(baseRelation: BaseRelation): DataFrame //外部数据源创建DataFrame
  *
  * 创建Dataset
  * def emptyDataset[T: Encoder]: Dataset[T]
  * def createDataset[T : Encoder](data: Seq[T]): Dataset[T]
  * def createDataset[T : Encoder](data: RDD[T]): Dataset[T]
  * def createDataset[T : Encoder](data: java.util.List[T]): Dataset[T]
  * def range(start: Long, end: Long, step: Long, numPartitions: Int): Dataset[java.lang.Long]
  */
object CreateDatasetApi {

  case class Person(val name: String)

  def testCreateDataset(spark: SparkSession): Unit = {
    val df: DataFrame = spark.createDataFrame(Seq(Person("Tom"),Person("Jack")))
    val df2: DataFrame = spark.createDataFrame(Seq("Chinese" -> 48, "English" -> 98))
    val df3: DataFrame = spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Person("Tom"),Person("Jack"))))

    import spark.implicits._ //需要通过隐式类型转换将 Product或基础类型->Encoder
    val ds: Dataset[Person] = spark.emptyDataset[Person] //需要通过隐式类型转换将Product->Encoder
    val ds1: Dataset[java.lang.Long] = spark.range(0, 10, 2, 2)
    val ds2: Dataset[(String, Int)] = spark.createDataset(Seq("Chinese" -> 48, "English" -> 98))
    val ds3: Dataset[Person] = spark.createDataset(spark.sparkContext.makeRDD(Seq(Person("Tom"),Person("Jack"))))
    val ds4: Dataset[String] = spark.createDataset(Seq("A","B")) //对比createDataFrame可不能创建这种基础类型的
  }

  def testConvert(spark: SparkSession): Unit = {
    import spark.implicits._

    //RDD转DataFrame
    val df: DataFrame = spark.sparkContext.makeRDD(Seq(Person("Tom"),Person("Jack"))).toDF()
    df.printSchema()
    df.show()

    //RDD转DataFrame带StructType
    val schema = StructType(StructField("subject", StringType, false) :: StructField("score", IntegerType, true) :: Nil)
    val rdd: RDD[Row] = spark.sparkContext.makeRDD(Seq("Chinese" -> 48, "English" -> 98)).map(tuple => { Row(tuple._1, tuple._2) })
    val df2: DataFrame = spark.createDataFrame(rdd, schema)
    df2.printSchema()
    df2.show()

    //RDD转Dataset
    val ds: Dataset[Person] = spark.createDataset(spark.sparkContext.makeRDD(Seq(Person("Tom"),Person("Jack"))))

    //DataFrame/Dataset转RDD
    ds.rdd

    //DatFrame转Dataset
    val ds2: Dataset[Person] = df.as[Person]
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Demo")
      .getOrCreate()
    testCreateDataset(spark)
    spark.stop()
  }
}
