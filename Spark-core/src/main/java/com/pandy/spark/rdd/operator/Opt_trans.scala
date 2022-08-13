package com.pandy.spark.rdd.operator

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Opt_trans {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")

        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        def mapFunction(num: Int): Int = {
            num * 2
        }

        // 转换算子
        rdd.map(mapFunction).foreach(println)
        rdd.map((num: Int) => {num * 2}).foreach(println)
        rdd.map(_*2).foreach(println)

        sc.stop()
    }
}
