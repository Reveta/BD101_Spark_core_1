package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class TaskUtil {
  def createRDD(sparkSession: SparkSession, path: String): RDD[Hotel] = {
    sparkSession
      .sparkContext
      .textFile(path)
      .filter(a => !Hotel.isHeaderCsv(a))
      .map(line => line.split(",", -1))
      .map((line) => Hotel.createHotel(line))
  }

  def task1(hotelsRDD: RDD[Hotel]): Array[((String, String, String), Int)] = {
    hotelsRDD
      .filter(_ != null)
      .filter(a => a.srch_adults_cnt.equals("2"))
      .groupBy(record => (record.hotel_continent, record.hotel_country, record.hotel_market))
      .map(kv => (kv._1, kv._2.count(kv => true)))
      .sortBy(_._2, false)
      .take(3)

  }


  def task2(hotelsRDD: RDD[Hotel]): Array[(String, Int)] = {
    hotelsRDD.filter(_ != null)
      .filter(a => a.srch_destination_id.equals(a.user_location_country))
      .groupBy(record => record.hotel_country)
      .map(kv => (kv._1, kv._2.size))
      .sortBy(_._2, false).take(1)

  }

  def task3(hotelsRDD: RDD[Hotel]): Array[((String, String, String), Int)] = {
    hotelsRDD.filter(_ != null)
      .filter(a => a.is_booking.equals("0") & !a.srch_children_cnt.equals("0"))
      .groupBy(record => (record.hotel_continent, record.hotel_country, record.hotel_market))
      .map(kv => (kv._1, kv._2.size))
      .sortBy(_._2, false).take(3)
  }


}
