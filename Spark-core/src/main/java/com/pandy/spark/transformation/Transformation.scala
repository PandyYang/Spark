package com.pandy.spark.transformation

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Transformation {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)

    System.setProperty("hadoop.home.dir", "E:\\wintuils\\")

    // 1. map
    var list = List(1, 2, 3)
    sc.parallelize(list).map(_ * 10).foreach(println)

    // 2. filter
    list = List(3, 6, 9, 10, 12, 21)
    sc.parallelize(list).filter(_ >= 10).foreach(println)

    // 3. flatMap
    val list2 = List(List(1, 2), List(3), List(4), List(), List(4, 5))
    sc.parallelize(list2).flatMap(_.toList).map(_ * 10).foreach(println)

    val lines = List("spark flume spark", "hadoop flume hive")
    sc.parallelize(lines).flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).foreach(println)

    // 4. mapPartitions
    val list4 = List(1, 2, 3, 4, 5, 6)

//    sc.parallelize(list, 3).mapPartitions(iterator => {
//      val buffer = new ListBuffer[Int]
//      while (iterator) {
//        buffer.append(iterator.next() * 100)
//      }
//    })

    // 5.sample 抽样
    // 第一个参数 是否放回
    // 第二个参数 抽样比例
    // 第三个参数 随机数种子
    val list5 = List(1, 2, 3, 4, 5, 6)
    sc.parallelize(list5).sample(withReplacement = false, fraction = 0.5).foreach(println)

    // 6.union
    val list61 = List(1, 2, 3)
    val list62 = List(4, 5, 6)
    sc.parallelize(list61).union(sc.parallelize(list62)).foreach(println)

  }

}
