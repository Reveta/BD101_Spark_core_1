package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession


  private val pathCSV: String = "src\\main\\scala\\resources\\train.csv"


  val hotelsRDD: RDD[Hotel] = Engine
    .createHotelsRDD(pathCSV)
    .cache()


  private val task1Result: Array[((Int, Int, Int), Int)] = Engine.task1(hotelsRDD)
  task1Result.foreach(println)

  private val task2Result: (Int, Int) = Engine.task2(hotelsRDD)
  println(task2Result.toString())

  private val task3Result: Array[((Int, Int, Int), Int)] = Engine.task3(hotelsRDD)
  task3Result.foreach(println)


  sparkSession.close()

}
