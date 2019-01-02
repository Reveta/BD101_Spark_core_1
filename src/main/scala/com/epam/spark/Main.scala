package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession
  var taskUtil = new TaskUtil
  var hotelsRDD: RDD[Hotel] = taskUtil.createRDD(sparkSession, "src/test/resources/test.csv")


//  val task1RDD: Array[((String, String, String), Int)] = taskUtil.task1(hotelsRDD)
//  task1RDD.foreach(println)
//  val task2RDD: Array[(String, Int)] = taskUtil.task2(hotelsRDD)
//  task2RDD.foreach(println)
  val task3RDD: Array[((String, String, String), Int)] = taskUtil.task3(hotelsRDD)
  task3RDD.foreach(println)


  sparkSession.close()

}
