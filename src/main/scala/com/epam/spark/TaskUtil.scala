package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

object TaskUtil {
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

  def task1(hotelsRDD: RDD[Option[Hotel]]): Array[((Int, Int, Int), Int)] = {
    hotelsRDD
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.srch_adults_cnt == 2)
      .map(record => ((record.hotel_continent, record.hotel_country, record.hotel_market), 1))
      .aggregateByKey(0)((a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)
      .sortBy(_._2, false)
      .take(3)

  }


  def task2(hotelsRDD: RDD[Option[Hotel]]): Array[(Int, Int)] = {
    hotelsRDD
      .filter(_.isDefined)
      .map(_.get)
      .filter(a => (a.srch_destination_id == a.user_location_country))
      .map(record => (record.hotel_country, 1))
      .aggregateByKey(0)((a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)
      .sortBy(_._2, false)
      .take(1)

  }

  def task3(hotelsRDD: RDD[Option[Hotel]]): Array[((Int, Int, Int), Int)] = {
    hotelsRDD
      .filter(_.isDefined)
      .map(_.get)
      .filter(a => (a.is_booking == 0 & a.srch_children_cnt != 0))
      .map(record => ((record.hotel_continent, record.hotel_country, record.hotel_market), 1))
      .aggregateByKey(0)((a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)
      .sortBy(_._2, false)
      .take(3)
  }


}
