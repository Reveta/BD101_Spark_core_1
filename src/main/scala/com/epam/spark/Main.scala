package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession

 val pathToCsv:String="src/main/scala/resources/train.csv"
  val rdd_records = sparkSession.sparkContext.textFile(pathToCsv)
  rdd_records.foreach(Hotel.parseLine)
  val splitted_csv_recordsRDD = rdd_records.map((line: String) => line.split(","))



  var hotelsRDD: RDD[Hotel] = TaskUtil.createRDD(sparkSession, "src/main/scala/resources/train.csv")


//  val task1RDD: Array[((Int, Int, Int), Int)] = TaskUtil.task1(hotelsRDD)
//  task1RDD.foreach(println)
//  val task2RDD: Array[(Int, Int)] = TaskUtil.task2(hotelsRDD)
//  task2RDD.foreach(println)
  val task3RDD: Array[((Int,Int,Int), Int)] = TaskUtil.task3(hotelsRDD)
  task3RDD.foreach(println)


  sparkSession.close()

}
