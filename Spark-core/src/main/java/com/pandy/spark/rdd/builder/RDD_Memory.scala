package com.pandy.spark.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_Memory {

    /**
     * 从内存中构建rdd
     * @param args
     */

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

        val sc = new SparkContext(sparkConf)

        // 创建RDD
        val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)

//        val rdd: RDD[Int] = sc.parallelize(seq)
        val rdd: RDD[Int] = sc.makeRDD(seq)

        rdd.collect().foreach(println)

        sc.stop()
    }

}
