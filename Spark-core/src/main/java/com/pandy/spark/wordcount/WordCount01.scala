package com.pandy.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount01 {
    def main(args: Array[String]): Unit = {

        // Application


        // Spark Framework
        val conf = new SparkConf().setMaster("local").setAppName("WordCount")
        val context = new SparkContext(conf)

        // close conn
        context.stop()
    }

}
