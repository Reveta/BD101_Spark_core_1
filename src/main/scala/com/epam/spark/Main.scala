package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession
  var trainRDD: RDD[String] = sparkSession.sparkContext.textFile("src\\main\\scala\\resources\\train.csv")

  private val value: RDD[Hotel] = trainRDD.map(Hotel(_))

  println(value.count())
  println(value.first())


  sparkSession.close()

}
