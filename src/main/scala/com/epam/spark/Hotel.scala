package com.epam.spark

import java.io.StringReader
import java.util


case class Hotel(
                  //TODO Проінспектувати індекси на валідність даних
                  srch_adults_cnt: Int,
                  hotel_continent: Int,
                  hotel_country: Int,
                  hotel_market: Int,
                  hotel_cluster: Int,
                  srch_children_cnt: Int,
                  is_booking: Int,
                  user_location_country: Int,
                  srch_destination_id: Int
                ) {

  override def toString = "Hotel(" +
    s" \n srch_adults_cnt = $srch_adults_cnt" +
    s",\n hotel_continent = $hotel_continent" +
    s",\n hotel_country = $hotel_country" +
    s",\n hotel_market = $hotel_market" +
    s",\n hotel_cluster = $hotel_cluster" +
    s",\n srch_children_cnt = $srch_children_cnt" +
    s",\n is_booking = $is_booking" +
    s",\n user_location_country = $user_location_country" +
    s",\n srch_destination_id = $srch_destination_id)\n \n"
}


object Hotel {

  val srch_adults_cnt = 13
  val hotel_continent = 18
  val hotel_country = 19
  val hotel_market = 20
  val hotel_cluster = 23
  val srch_children_cnt = 14
  val is_booking = 21
  val user_location_country = 3
  val srch_destination_id = 16


  def isHeaderCsv(line: String): Boolean = line.startsWith("date_time,site_name")

  def apply(row: String): Hotel = {
    print("[INFO] Start creating Hotel - " + row + "\n")
    var parser: List[String] = null
    try {
      parser = new Parser().parseLine(row)
    } catch {
      case e: Exception => print("[ERROR]CastException! - " + row + "\n")
    }

    //TODO Проінспектувати індекси на валідність даних
    val getCol: Int => String = parser(_)
    val hotel: Hotel = new Hotel(
      toInt(getCol(srch_adults_cnt)),
      toInt(getCol(hotel_continent)),
      toInt(getCol(hotel_country)),
      toInt(getCol(hotel_market)),
      toInt(getCol(hotel_cluster)),
      toInt(getCol(srch_children_cnt)),
      toInt(getCol(is_booking)),
      toInt(getCol(user_location_country)),
      toInt(getCol(srch_destination_id)))

    return hotel
  }

  private def toInt(input: String): Int = {
    val checkIsNotNull: String = if (input.length != 0) input else "0"

    var toInt = 0
    try {
      return Integer.valueOf(checkIsNotNull)
    } catch {
      case e: IllegalArgumentException => print("Arguments exeption - [ " + input + " ] to Integer \n")
    }

    return toInt
  }
}