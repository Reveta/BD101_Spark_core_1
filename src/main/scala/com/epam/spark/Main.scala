package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession
  var trainRDD: RDD[String] = sparkSession
    .sparkContext
    .textFile("src\\main\\scala\\resources\\train.csv")

  val splitted_csv_recordsRDD: RDD[Array[String]] = trainRDD.
    filter(a => !Hotel.isHeaderCsv(a))
    .map(line => line.split(",", -1))

  val hotelsRDD: RDD[Hotel] = splitted_csv_recordsRDD
    .map((line) => Hotel.createHotel(line))


  val task1RDD: RDD[(String,Int)] = hotelsRDD.filter(_!=null)
    .filter(a => a.srch_adults_cnt.equals("2"))
    .groupBy(_.hotel_continent)
   .map(kv => (kv._1, kv._2.size))
    .sortBy(_._2, false)



  task1RDD.foreach(println)


  // private val topHotelsDuoRDD: RDD[(Int, Int)] = hotelDuoRDD
  //    .groupBy(_.hotel_continent)
  //    .map(kv => (kv._1, kv._2.size))
  //    .sortBy(_._2, false)


  sparkSession.close()

}
