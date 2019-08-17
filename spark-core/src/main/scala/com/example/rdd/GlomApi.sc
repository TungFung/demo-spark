import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

val conf: SparkConf = new SparkConf().setMaster("local[4]").setAppName("Demo")
val sc: SparkContext = new SparkContext(conf)
val inputRDD: RDD[String] = sc.makeRDD(Seq("Chinese","Math","English"), numSlices = 10)
inputRDD.glom.collect
sc.stop