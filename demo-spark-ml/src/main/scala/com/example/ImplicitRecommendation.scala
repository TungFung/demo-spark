package com.example

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.functions.{lit, max, min}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.util.Random

/**
 *  spark-shell --master spark://master:7077 --executor-memory 512M --executor-cores 1 --total-executor-cores 2
 *  隐式类型数据的推荐实现
 */
object ImplicitRecommendation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs://master:9999/user/hadoop/als/tmp")

    import spark.implicits._

    //艺术家名字修正表
    val artistAlias: Dataset[String] = spark.read.textFile("hdfs://master:9999/user/hadoop/als/artist_alias.txt")
    val artistAliasMap: Map[Int, Int] = artistAlias.flatMap { line =>
      val Array(artistId, aliasId) = line.split("\t")
      if(artistId.isEmpty) {
        None
      } else{
        Some(artistId.toInt, aliasId.toInt)
      }
    }.collect().toMap
    val bArtistAliasMap: Broadcast[Map[Int, Int]] = spark.sparkContext.broadcast(artistAliasMap) //广播

    //用户对艺术家的播放次数
    val userArtistData: Dataset[String] = spark.read.textFile("hdfs://master:9999/user/hadoop/als/user_artist_data.txt")
    val userArtistDF: DataFrame = userArtistData.map { line =>
      val Array(userId, artistId, count) = line.split(" ")
      val finalArtistId = bArtistAliasMap.value.getOrElse(artistId.toInt, artistId.toInt)
      (userId.toInt, finalArtistId, count.toInt)
    }.toDF("userId", "artistId", "count")
    val trainData = userArtistDF.cache() //缓存训练数据

    //交替最小二乘法（ALS）推荐算法
    val model: ALSModel = new ALS()
      .setSeed(Random.nextLong()) //随机种子数
      .setImplicitPrefs(true) //我们这里的数据是隐式数据集
      .setRank(10) //隐语义因子的个数，默认是10
      .setRegParam(0.01) //指定ALS的正则化参数，默认是1.0，这个值越大可以有效的防止过拟合，但是越大的话会影响矩阵分解的精度
      .setAlpha(1.0) //置信度的增长速度。对于这个参数的解释可以参考：https://amsterdam.luminis.eu/2016/12/04/alternating-least-squares-implicit-feedback-search-alpha/
      .setMaxIter(5) //迭代的次数
      .setUserCol("userId") //用户列
      .setItemCol("artistId") //产品列
      .setRatingCol("count") //评分列
      .setPredictionCol("prediction") //预测值列
      .fit(trainData) //模型训练

    model.userFactors.show(1, truncate = false)//查看用户与隐语义因子矩阵 id,features
    model.itemFactors.show(1, truncate = false)//查看产品与隐语义因子矩阵 id,features

    //以这个用户为例子校验推荐结果
    val userId: Int = 2093760

    //从模型中的艺术家特征矩阵中查出所有的艺术家的id，然后拼上userId。即 artistId, userId
    val toRecommend: DataFrame = model.itemFactors.select($"id" as("artistId")).withColumn("userId", lit(userId))

    //模型转换, 即artistId, userId, prediction
    val transformModel: DataFrame = model.transform(toRecommend)

    //将上面得到的数据放到模型中进行计算，然后计算出按照推荐分数降序排，取指定的前几个。即 artistId, prediction
    val recommendation: DataFrame = transformModel.select("artistId", "prediction").orderBy($"prediction".desc).limit(5)

    //推荐出来的艺术家Id
    val recommendArtistIds: Array[Int] = recommendation.select("artistId").as[Int].collect()

    //艺术家id以及名字
    val artistData: Dataset[String] = spark.read.textFile("hdfs://master:9999/user/hadoop/als/artist_data.txt")
    val artistDF = artistData.map { line =>
      val Array(artistId, artistName) = line.split('\t')
      (artistId, artistName)
    }.filter(!_._1.isEmpty).map{ artist =>
      (artist._1.toInt, artist._2.toString.trim)
    }.toDF("artistId", "artistName")
    artistDF.filter($"artistId" isNull).show()
    artistDF.filter($"artistId" isin (recommendArtistIds: _*)).show()

    //释放内存
    trainData.unpersist()
    model.userFactors.unpersist()
    model.itemFactors.unpersist()
  }

}
