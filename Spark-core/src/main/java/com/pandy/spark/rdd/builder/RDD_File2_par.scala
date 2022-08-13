package com.pandy.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_File2_par {

    /**
     * 从文件中构建rdd
     * @param args
     */

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

        val sc = new SparkContext(sparkConf)

        // textFile可以将文件作为数据处理的数据源 也可以设置默认分区
        val rdd: RDD[String] = sc.textFile("data/1.txt", 3)

        rdd.saveAsTextFile("output")

        sc.stop()
    }

}
