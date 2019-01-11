package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession

  //src/main/scala/resources/train.csv
  val pathToCsv: String = args(0)
  //creating rdd from a set of hotels, that are defined from csv file
  val hotelsRDD: RDD[Option[Hotel]] = TaskUtil.createRDD(sparkSession, pathToCsv)
  //filtering rdd from null hotels
  val notNullHotelsRDD: RDD[Hotel] = TaskUtil.filterNullHotelsRDD(hotelsRDD).cache()

  val task1RDD: Array[((Int, Int, Int), Int)] = TaskUtil.task1(notNullHotelsRDD)
  //printing results of TaskUtil.task1
  task1RDD.foreach(println)
  val task2RDD: Array[(Int, Int)] = TaskUtil.task2(notNullHotelsRDD)
  //printing results of TaskUtil.task2
  task2RDD.foreach(println)
  val task3RDD: Array[((Int, Int, Int), Int)] = TaskUtil.task3(notNullHotelsRDD)
  //printing results of TaskUtil.task3
  task3RDD.foreach(println)


  sparkSession.close()

}
