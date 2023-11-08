package com.pandy.spark.rdd.create

import org.apache.spark.{SparkConf, SparkContext}

object Test01_FromList {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkCore").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val list = List(1, 2, 3, 4)

    val intRDD = sc.parallelize(list)


  }
}
