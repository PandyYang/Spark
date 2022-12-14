package com.pandy.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount01 {
    def main(args: Array[String]): Unit = {

        // Application
        // Spark Framework
        val conf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
        val sc = new SparkContext(conf)

        // work
        val lines: RDD[String] = sc.textFile(path = "data")

        // split line 扁平化
        val words: RDD[String] = lines.flatMap(_.split(" "))

        // group word
        val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (x, y) =>
                (x, y.size)
        }

        wordToCount.foreach(println)

        // close conn
        sc.stop()
    }

}
