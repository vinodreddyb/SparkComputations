package com.vinod.spark.compute

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.util.Try

/**
  * Created by 391633 on 10/31/2016.
  */
object SparkAverage {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutils")
    val conf = new SparkConf().setAppName("AverageExample").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("test.txt")

    val split = rdd.map { line =>

      val record = line.split(",")
      (record(0), Try(record(1).toDouble).getOrElse(0.0))
    }.cache

    // Using aggregateByKey function
    val avgByAggregateByKey = split.aggregateByKey((0.0, 0))(
      (accum, value) => (accum._1 + value, accum._2 + 1),
      (accum1, accum2) => (accum1._1 + accum2._1, accum1._2 + accum2._2)
    ).mapValues {
      avg =>
        avg._1 / avg._2
    }
    avgByAggregateByKey.foreach(println)

    // Using combiner function
    val avgByCombineByKey = split.combineByKey((score: Double) => (score, 1),   /*Create combiner*/
      (accum: (Double, Int), value: Double) => (accum._1 + value, accum._2 + 1), /*Create merge value*/
      (accum1: (Double, Int), accum2: (Double, Int)) => (accum1._1 + accum2._1, accum1._2 + accum2._2)) /*Merge combiner*/
      .mapValues {
        avg => avg._1 / avg._2
      }

    avgByCombineByKey.foreach(println)

    // Using reduceByKey
    val avgByReduceByKey = split.mapValues(mark => (mark, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(avg => avg._1 / avg._2)

    avgByReduceByKey.foreach(println)

  }


}
