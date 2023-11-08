package com.pandy.spark.project01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test01_Top10 {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkCore").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val lineRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

    val clickRDD: RDD[String] = lineRDD.filter(line => {
      val data: Array[String] = line.split("_")
      data(6) != "-1"
    })

    // 统计品类点击次数
    val clickCountRDD: RDD[(String, Int)] = clickRDD.map(line => {
      val data: Array[String] = line.split("_")
      (data(6), 1)
    }).reduceByKey(_ + _)

    // 过滤统计下单品类
    val orderRDD: RDD[String] = lineRDD.filter(line => {
      val data: Array[String] = line.split("_")
      data(8) != "null"
    })


  }
}
