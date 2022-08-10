package com.pandy.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memory_parall {

    /**
     * 从内存中构建rdd
     * rdd并行度&分区
     * @param args
     */

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

        val sc = new SparkContext(sparkConf)

        // makeRDD 第二个参数 指定分区
        val rdd: RDD[Int] = sc.makeRDD(
            List(1,2,3,4), 2
        )

        // 将处理的数据保存成分区文件
        rdd.saveAsTextFile("output")

        sc.stop()
    }

}
