package com.epam.spark

import java.io.StringReader
import java.util

object HotelBuilder {
  val  hotel_continent = 19
  val hotel_country = 20
  val hotel_market = 21
  val srch_adults_cnt = 14
  val srch_children_cnt = 15
  val is_booking = 22
  val user_location_country = 4
  val srch_destination_id = 17


  def isHeaderCsv(line: String): Boolean = line.startsWith("date_time,site_name")

  def createHotel(row: String): Hotel = {
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
