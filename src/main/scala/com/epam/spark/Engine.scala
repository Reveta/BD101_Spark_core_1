package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Engine {

  val spark: SparkSession = SparkConfiguration.sparkSession

  def createHotelsRDD(path: String): RDD[Hotel] = {
    return spark.sparkContext
      .textFile(path)
      .filter(a => !Hotel.isHeaderCsv(a))
      .map(Hotel(_))
  }

  def task1(hotelsRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    println("\n[INFO] Task 1 start")
    val notNullHotelRDD: RDD[Hotel] = hotelsRDD
      .filter(_ != null)

    val duoHotelsRDD: RDD[Hotel] = notNullHotelRDD
      .filter(a => a.srch_adults_cnt == 2)

    val groupByHotelsRDD: RDD[((Int, Int, Int), Int)] = duoHotelsRDD
      .groupBy(hotel => (hotel.hotel_continent, hotel.hotel_country, hotel.hotel_market))
      .map(kv => (kv._1, kv._2.size))

    val topTreeDuoHotels: Array[((Int, Int, Int), Int)] = groupByHotelsRDD
      .sortBy(_._2, false)
      .take(3)


    println("[INFO] Task 1 result -")
    if (topTreeDuoHotels.length != 0) {
      return topTreeDuoHotels
    } else {
      println("[WARN] Task 1 - result is null")
      return topTreeDuoHotels
    }
  }


  def task2(hotelsRDD: RDD[Hotel]): (Int, Int) = {
    println("\n[INFO] Task 2 start")
    val notNullHotelRDD: RDD[Hotel] = hotelsRDD
      .filter(_ != null)

    val groupHotelsRDD: RDD[(Int, Int)] = notNullHotelRDD
      .filter(a => a.srch_destination_id.equals(a.user_location_country))
      .groupBy(_.hotel_country)
      .map(kv => (kv._1, kv._2.size))

    val result: Array[(Int, Int)] = groupHotelsRDD
      .sortBy(_._2, false)
      .take(1)


    println("[INFO] Task 2 result -")
    if (result.length != 0) {
      return result(0)
    } else {
      println("[WARN] Task 2 - result is null")
      return (0, 0)
    }
  }


  def task3(hotelsRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    println("\n[INFO] Task 3 start")
    val notNullHotelRDD: RDD[Hotel] = hotelsRDD
      .filter(_ != null)

    val sortedHotelsRDD: RDD[((Int, Int, Int), Int)] = notNullHotelRDD
      .filter(a => a.is_booking == 0 && !(a.srch_children_cnt == 0))
      .groupBy(record => (record.hotel_continent, record.hotel_country, record.hotel_market))
      .map(kv => (kv._1, kv._2.size))

    val result: Array[((Int, Int, Int), Int)] = sortedHotelsRDD
      .sortBy(_._2, false)
      .take(3)


    if (result.length == 0) {
      println("[WARN] Task 3 - result is null")
    }
    println("[INFO] Task 3 result -")
    return result
  }

}