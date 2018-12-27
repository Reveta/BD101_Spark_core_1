package com.epam.spark

import java.io.StringReader
import java.util

object HotelBuilder {
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
