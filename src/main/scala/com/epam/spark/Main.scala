package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession
  var trainRDD: RDD[String] = sparkSession.sparkContext.textFile("src/resorses/train.csv")

  private val value: RDD[Hotel] = trainRDD.map(a => HotelBuilder.createHotel(a))

  println(value.count())
  println(value.first())



  sparkSession.close()

}
