package com.example.streaming

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Counts words in new text files created in the given directory
  * 1、监控目录下的文件的格式必须是统一的
  * 2、不支持嵌入文件目录
  * 3、一旦文件移动到这个监控目录下，是不能变的，往文件中追加的数据是不会被读取的
  * spark-shell --master spark://master:7077 --total-executor-cores 4 --executor-cores 2
  * hadoop fs -copyFromLocal test1-process.txt hdfs://master:9999/streaming
  */
object HdfsFileStreamWordCount {

  private val FILE_PATH = "hdfs://master:9999/streaming"

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("File Stream Word Count")

    val sc: SparkContext = new SparkContext(conf)

    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))

    //@param directory HDFS directory to monitor for new file
    //@param filter Function to filter paths to process
    //@param newFilesOnly Should process only new files and ignore existing files in the directory
    val lines: DStream[(LongWritable, Text)] = ssc.fileStream[LongWritable, Text, TextInputFormat](
      FILE_PATH,
      (path: Path) => path.toString.contains("process"),
      false)

    //lines.print() 这里不能print，因为LongWritable没有实现Serializable

    val texts: DStream[String] = lines.map(_._2.toString)
    texts.print()

    val words: DStream[String] = texts.flatMap(_.split(" "))

    val wordPairs: DStream[(String, Int)] = words.map(e => (e, 1))

    val wordCounts: DStream[(String, Int)] = wordPairs.reduceByKey(_ + _)

    wordCounts.print()

    ssc.start()

    ssc.awaitTermination()
  }

}
