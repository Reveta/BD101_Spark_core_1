package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession
  var trainRDD: RDD[String] = sparkSession.sparkContext.textFile("src\\main\\scala\\resources\\train.csv")


  private val hotelsRDD: RDD[Hotel] = trainRDD
    .filter(a => !Hotel.isHeaderCsv(a))
    .map(Hotel(_))

  private var hotelDuoRDD: RDD[Hotel] = hotelsRDD
    .filter(a => a.srch_adults_cnt == 2)

  private val topHotelsDuoRDD: RDD[(Int, Int)] = hotelDuoRDD
    .groupBy(_.hotel_continent)
    .map(kv => (kv._1, kv._2.size))
    .sortBy(_._2, false)

  topHotelsDuoRDD.take(3).foreach(print)

  sparkSession.close()

}
