package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Engine {

  val spark: SparkSession = SparkConfiguration.sparkSession

  /** logic for aggregateByKey() */
  val initialCount: Int = 0
  val addToCounts: (Int, Int) => Int = (hotelValue1, hotelValue2) => hotelValue1 + hotelValue2
  val sumCountsFromPartions: (Int, Int) => Int = (count1, count2) => count1 + count2


  /** Creates RDD from a set of hotels, read from csv file
    *
    * @return hotelRDD for all tasks */
  def createHotelsRDD(path: String): RDD[Hotel] = {
    val saveHotelRDD: RDD[Option[Hotel]] = spark.sparkContext
      .textFile(path)
      .filter(line => !Hotel.isHeaderCsv(line))
      .map(Hotel(_))

    val hotelRDD: RDD[Hotel] = saveHotelRDD
      .filter(opt => opt.isDefined)
      .map(opt => opt.get)


    return hotelRDD
  }

  /** Logic of task1
    * Finds top 3 most popular hotels between couples(treats hotel as composite key of continent, country and market).
    *
    * @return RDD - top 3 most popular hotels between couples
    * @return If answer is empty - return empty rdd */
  def task1(hotelsRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    println("\n[INFO] Task 1 start")

    val duoHotelsRDD: RDD[Hotel] = hotelsRDD
      .filter(_.srch_adults_cnt == 2)

    val groupByHotelsIdRDD: RDD[((Int, Int, Int), Int)] = duoHotelsRDD
      .map(hotel => ((hotel.hotel_continent, hotel.hotel_country, hotel.hotel_market), 1))
      .aggregateByKey(initialCount)(addToCounts, sumCountsFromPartions)

    val topTreeDuoHotels: Array[((Int, Int, Int), Int)] = groupByHotelsIdRDD
      .sortBy(_._2, false)
      .take(3)



    /* If answer is empty - return empty rdd */
    println("[INFO] Task 1 result -")
    if (topTreeDuoHotels.length != 0) {
      return topTreeDuoHotels
    } else {
      println("[WARN] Task 1 - result is null")
      return topTreeDuoHotels
    }
  }

  /** Logic of task2
    *
    * @return RDD - the most popular country where hotels are booked and searched from the same country
    * @return If answer is empty - return (0,0) */
  def task2(hotelsRDD: RDD[Hotel]): (Int, Int) = {
    println("\n[INFO] Task 2 start")

    val groupHotelsRDD: RDD[(Int, Int)] = hotelsRDD
      .filter(hotel => hotel.srch_destination_id == hotel.user_location_country)
      .map(hotel => (hotel.hotel_country, 1))
      .aggregateByKey(initialCount)(addToCounts, sumCountsFromPartions)

    val result: Array[(Int, Int)] = groupHotelsRDD
      .sortBy(_._2, false)
      .take(1)

    /*If answer is empty - return (0,0)*/
    println("[INFO] Task 2 result -")
    if (result.length != 0) {
      return result(0)
    } else {
      println("[WARN] Task 2 - result is null")
      return (0, 0)
    }
  }

  /** Logic of task2
    *
    * @return RDD - top 3 hotels where people with children are interested but not booked in the end
    * @return If answer is empty - return empty rdd */
  def task3(hotelsRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    println("\n[INFO] Task 3 start")

    val sortedHotelsRDD: RDD[((Int, Int, Int), Int)] = hotelsRDD
      .filter(hotel => hotel.is_booking == 0 && !(hotel.srch_children_cnt == 0))
      .map(hotel => ((hotel.hotel_continent, hotel.hotel_country, hotel.hotel_market), 1))
      .aggregateByKey(initialCount)(addToCounts, sumCountsFromPartions)

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