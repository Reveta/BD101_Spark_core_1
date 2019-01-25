package com.epam.spark

import org.apache.spark.rdd.RDD

/** Start point*/
object Main extends App {

  val defCSVDataFilePath = "src\\main\\scala\\resources\\trainSmall.csv"
  val sparkSession = SparkConfiguration.sparkSession

  /** way to csv data file */
  private val pathCSV = if (args.length == 0) defCSVDataFilePath else args(0)


  /** generate and cache hotelRDD from csv file */
  val hotelsRDD: RDD[Hotel] = Engine
    .createHotelsRDD(pathCSV)
    .cache()

  /** start tasks 1 and show result */
  private val task1Result: Array[((Int, Int, Int), Int)] = Engine.task1(hotelsRDD)
  task1Result.foreach(println)

  /** start tasks 2 and show result */
  private val task2Result: (Int, Int) = Engine.task2(hotelsRDD)
  println(task2Result.toString())

  /** start tasks 3 and show result */
  private val task3Result: Array[((Int, Int, Int), Int)] = Engine.task3(hotelsRDD)
  task3Result.foreach(println)


  sparkSession.close()
}
