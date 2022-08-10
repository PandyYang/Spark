package com.pandy.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_File1 {

    /**
     * 从文件中构建rdd
     * @param args
     */

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

        val sc = new SparkContext(sparkConf)

        // 创建RDD path默认以当前环境的根路径为基准 可以写绝对路径 也可以相对路径
        // 也能够通配符 还能是分布式存储系统路径
        // 以文件为单位读取数据
        val rdd: RDD[(String, String)] = sc.wholeTextFiles("data")

        rdd.collect().foreach(println)

        sc.stop()
    }

}
