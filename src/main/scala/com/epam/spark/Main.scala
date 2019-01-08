package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession

  val pathToCsv: String = "src/main/scala/resources/train.csv"
  val hotelsRDD: RDD[Option[Hotel]] = TaskUtil.createRDD(sparkSession, pathToCsv).cache()


    val task1RDD: Array[((Int, Int, Int), Int)] = TaskUtil.task1(hotelsRDD)
    task1RDD.foreach(println)
    val task2RDD: Array[(Int, Int)] = TaskUtil.task2(hotelsRDD)
    task2RDD.foreach(println)
  val task3RDD: Array[((Int, Int, Int), Int)] = TaskUtil.task3(hotelsRDD)
  task3RDD.foreach(println)


  sparkSession.close()

}
