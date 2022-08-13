package com.pandy.spark.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Opt_trans_map_par {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(sparkConf)

        val rdd = sc.makeRDD(List(1,2,3,4), 2)

        val mapRDD: RDD[Int] = rdd.mapPartitions(
            line => {
                println("----------")
                line.map(_ * 2)
            }
        )

        mapRDD.collect().foreach(println)

        sc.stop()
    }
}
