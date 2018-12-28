package com.epam.spark

import java.io.StringReader
import java.util

import org.apache.spark.sql.SparkSession


case class Hotel(sp: SparkSession, srch_adults_cntI: String, hotel_continentI: String, hotel_countryI: String, hotel_marketI: String, hotel_clusterI: String) {
  private var spark: SparkSession = sp

  var srch_adults_cnt: String = srch_adults_cntI
  var hotel_continent: String = hotel_continentI
  var hotel_country: String = hotel_countryI
  var hotel_market: String = hotel_marketI
  var hotel_cluster: String = hotel_clusterI

}

object Hotel {

  val hotel_continent = 18
  val hotel_country = 19
  val hotel_market = 20
  val srch_adults_cnt = 13
  val srch_children_cnt = 14
  val is_booking = 21
  val user_location_country = 3
  val srch_destination_id = 16


  def isHeaderCsv(line: String): Boolean = line.startsWith("date_time,site_name")

  def apply(row: String): Hotel = {
    var parser: util.ArrayList[String] = null
    try {
      parser = new Parser().parseLine(new StringReader(row))
    } catch {
      case e: Exception => print("Exception!")
    }

    val getCol: Int => String = parser.get _
    //    val toInt: String => Int =
    return new Hotel(
      SparkConfiguration.sparkSession,
      (getCol(1)),
      (getCol(4)),
      (getCol(5)),
      (getCol(6)),
      (getCol(7)))

  }
}