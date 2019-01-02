package com.epam.spark

import org.apache.spark.rdd.RDD


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession
  var taskUtil = new TaskUtil
  var trainRDD: RDD[String] = taskUtil.createRDD(sparkSession,"src\\main\\scala\\resources\\train.csv")

  val splitted_csv_recordsRDD: RDD[Array[String]] = trainRDD
    .filter(a => !Hotel.isHeaderCsv(a))
    .map(line => line.split(",", -1))

  val hotelsRDD: RDD[Hotel] = splitted_csv_recordsRDD
    .map((line) => Hotel.createHotel(line))


  val task1RDD: Array[((String, String, String), Int)] = hotelsRDD
    .filter(_ != null)
    .filter(a => a.srch_adults_cnt.equals("2"))
    .groupBy(record => (record.hotel_continent, record.hotel_country, record.hotel_market))
    .map(kv => (kv._1, kv._2.count(kv => true)))
    .sortBy(_._2, false)
    .take(3)


  task1RDD.foreach(println)
  //  val task2RDD: Array[(String, Int)] = hotelsRDD.filter(_ != null)
  //    .filter(a => a.srch_destination_id.equals(a.user_location_country))
  //    .groupBy(record => record.hotel_country)
  //    .map(kv => (kv._1, kv._2.size))
  //    .sortBy(_._2, false).take(1)
  //
  //  task2RDD.foreach(println)
  //
  //  val task3RDD: Array[((String, String, String), Int)] = hotelsRDD.filter(_ != null)
  //    .filter(a => a.is_booking.equals("0") & !a.srch_children_cnt.equals("0"))
  //    .groupBy(record => (record.hotel_continent, record.hotel_country, record.hotel_market))
  //    .map(kv => (kv._1, kv._2.size))
  //    .sortBy(_._2, false).take(3)
  //
  //  task3RDD.foreach(println)
  //

  sparkSession.close()

}
