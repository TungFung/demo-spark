package com.example.streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object KafkaStreamWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("Kafka Stream Word Count")

    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(3))

    val topicSet: Set[String] = Set("topic1")

    val kafkaParams: Map[String, Object] = Map(
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "auto.offset.reset" -> "latest",
      "group.id" -> "test-consumer-group",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer]
    )

    val records: DStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, Subscribe[String, String](topicSet, kafkaParams))
    records.map(record => (record.value().toString)).print

    ssc.start()

    ssc.awaitTermination()
  }

}
