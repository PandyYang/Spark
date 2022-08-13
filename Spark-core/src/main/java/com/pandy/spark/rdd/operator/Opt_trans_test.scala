package com.pandy.spark.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Opt_trans_test {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("data/apache.log")

        rdd.map(
            line => {
                line.split(" ")(6)
            }
        )

        rdd.collect().foreach(println)

        sc.stop()
    }
}
