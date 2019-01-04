package com.epam.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TaskUtil {
  def createRDD(sparkSession: SparkSession, path: String): RDD[Hotel] = {
    sparkSession
      .sparkContext
      .textFile(path)
      .filter(a => !Hotel.isHeaderCsv(a))
      .
  }

  val initialcCount = 0
  val addToCounts = (a:Int,b:Int)=>a+b
  val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2


  def task1(hotelsRDD: RDD[Hotel]): Array[((Int, Int, Int),Int)] = {
    hotelsRDD
      .filter(_ != null)
      .filter(a => a.srch_adults_cnt == 2)
      .map(record => ((record.hotel_continent, record.hotel_country, record.hotel_market),1))
      .aggregateByKey(initialcCount)(addToCounts, sumPartitionCounts)
      .sortBy(_._2, false)
      .take(3)

  }


    def task2(hotelsRDD: RDD[Hotel]): Array[(Int, Int)] = {
      hotelsRDD.filter(_ != null)
        .filter(a => (a.srch_destination_id == a.user_location_country))
        .map(record => (record.hotel_country,1))
        .aggregateByKey(initialcCount)(addToCounts, sumPartitionCounts)
        .sortBy(_._2, false)
        .take(1)

    }

    def task3(hotelsRDD: RDD[Hotel]): Array[((Int,Int,Int), Int)] = {
      hotelsRDD.filter(_ != null)
        .filter(a =>( a.is_booking == 0 & a.srch_children_cnt !=0))
        .map(record => ((record.hotel_continent, record.hotel_country, record.hotel_market),1))
        .aggregateByKey(initialcCount)(addToCounts, sumPartitionCounts)
        .sortBy(_._2, false)
        .take(3)
    }


}
