package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession
  var trainRDD: RDD[String] = sparkSession.sparkContext.textFile("src\\main\\scala\\resources\\train.csv")


  // .filter(a => !Hotel.isHeaderCsv(a))


  val splitted_csv_recordsRDD: RDD[Array[String]] = trainRDD.map(line => line.split(",",-1))



  val avocadosRDD = splitted_csv_recordsRDD.map((line) => Hotel.createHotel(line))
  avocadosRDD.take(20).foreach(println)


  //val avocadosRDD: RDD[Hotel] = splitted_csv_recordsRDD.map(l=>Hotel(l))


  sparkSession.close()

}
