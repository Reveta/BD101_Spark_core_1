package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object TaskUtil {

  /**
    * Creates RDD from a set of hotels, read from csv file
    *
    * @param sparkSession
    * @param path - path to csv file
    * @return rdd of Option[Hotel]
    */
  def createRDD(sparkSession: SparkSession, path: String): RDD[Option[Hotel]] = {
    val rdd =
      sparkSession
        .sparkContext
        .textFile(path)
        .filter(a => !Hotel.isHeaderCsv(a))
        .map(a => Hotel.createHotel(a))
    val accum: LongAccumulator = sparkSession.sparkContext.longAccumulator
    rdd.filter(_.isEmpty).foreach((x) => accum.add(1))
    System.out.println("count of errors while parsing= " + accum.value)
    return rdd
  }


  /**
    * Creates RDD from a set of hotels, read from csv file
    *
    * @param sparkSession
    * @param path - path to csv file
    * @return rdd of Option[Hotel]
    */
  def filterNullHotelsRDD(hotelsRDD: RDD[Option[Hotel]]): RDD[Hotel] = {
    hotelsRDD
      .filter(_.isDefined)
      .map(_.get)
  }

  /**
    * Finds top 3 most popular hotels between couples(treats hotel as composite key of continent, country and market).
    *
    * @param hotelsRDD - input rdd
    * @return Array[((k1, k2, k3), count)], where (k1, k2, k3) is a composite key of continent, country and market
    *         and count is this hotel`s frequency in rdd.
    **/
  def task1(notNullHotelsRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    notNullHotelsRDD
      .filter(_.srch_adults_cnt == 2)
      .map(record => ((record.hotel_continent, record.hotel_country, record.hotel_market), 1))
      .aggregateByKey(0)((a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)
      .sortBy(_._2, false)
      .take(3)

  }

  /**
    * Finds the most popular country where hotels are booked and searched from the same country
    *
    * @param hotelsRDD - input rdd
    * @return Array[(k1, count)] where k1 is hotel country and count is this hotel`s frequency in rdd.
    **/

  def task2(notNullHotelsRDD: RDD[Hotel]): Array[(Int, Int)] = {
    notNullHotelsRDD
      .filter(a => (a.srch_destination_id == a.user_location_country))
      .map(record => (record.hotel_country, 1))
      .aggregateByKey(0)((a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)
      .sortBy(_._2, false)
      .take(1)

  }

  /**
    * Finds top 3 hotels where people with children are interested but not booked in the end
    *
    * @param hotelsRDD - input rdd
    * @return Array[((k1, k2, k3), count)], where (k1, k2, k3) is a composite key of continent, country and market
    *         and count is this hotel`s frequency in rdd.
    **/

  def task3(notNullHotelsRDD: RDD[Hotel]): Array[((Int, Int, Int), Int)] = {
    notNullHotelsRDD
      .filter(a => (a.is_booking == 0 & a.srch_children_cnt != 0))
      .map(record => ((record.hotel_continent, record.hotel_country, record.hotel_market), 1))
      .aggregateByKey(0)((a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)
      .sortBy(_._2, false)
      .take(3)
  }


}
