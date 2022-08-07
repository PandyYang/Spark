package com.pandy.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount03 {
    def main(args: Array[String]): Unit = {

        // Application
        // Spark Framework
        val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(conf)

        // work
        val lines: RDD[String] = sc.textFile(path = "data")

        // split line 扁平化
        val words: RDD[String] = lines.flatMap(_.split(" "))

        val wordToOne = words.map(
            word => (word, 1)
        )

        // reduceByKey 相同的key可以对value进行reduce聚合
        val wordToCount: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)

        wordToCount.foreach(println)

        // close conn
        sc.stop()
    }

}
