package com.pandy.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount02 {
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

        val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(
            t => t._1
        )

        val wordToCount: RDD[(String, Int)] = wordGroup.map {
            case (word, list) => {
                // tuple (word, wordcount)
                list.reduce(
                    (t1, t2) => {
                        (t1._1, t1._2 + t2._2)
                    }
                )
            }
        }

        wordToCount.foreach(println)

        // close conn
        sc.stop()
    }

}
