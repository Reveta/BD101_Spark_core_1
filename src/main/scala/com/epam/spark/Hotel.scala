package com.epam.spark


case class Hotel(
                  srch_adults_cnt: Int,
                  hotel_continent: Int,
                  hotel_country: Int,
                  hotel_market: Int,
                  hotel_cluster: Int,
                  srch_children_cnt: Int,
                  is_booking: Int,
                  user_location_country: Int,
                  srch_destination_id: Int

                )

object Hotel {

  val ADULTS = 13
  val HOTEL_CONTINENT = 20
  val HOTEL_COUNTRY = 21
  val HOTEL_MARKET = 22
  val HOTEL_CLUSTER = 23
  val CHILDREN = 14
  val IS_BOOKING = 18
  val USER_LOCATION_COUNTRY = 3
  val DESTINATION_ID = 16


  def isHeaderCsv(line: String): Boolean = line.startsWith("date_time,site_name")

  def createHotel(line: String): Option[Hotel]=
    try {
      val metadata: Array[String] = parseLine(line)
      val srch_adults_cnt: Int = metadata(ADULTS).toInt
      val hotel_continent: Int = metadata(HOTEL_CONTINENT).toInt
      val hotel_country: Int = metadata(HOTEL_COUNTRY).toInt
      val hotel_market: Int = metadata(HOTEL_MARKET).toInt
      val hotel_cluster: Int = metadata(HOTEL_CLUSTER).toInt
      val srch_children_cnt: Int = metadata(CHILDREN).toInt
      val is_booking: Int = metadata(IS_BOOKING).toInt
      val user_location_country: Int = metadata(USER_LOCATION_COUNTRY).toInt
      val srch_destination_id: Int = metadata(DESTINATION_ID).toInt

      val hotel = new Hotel(srch_adults_cnt, hotel_continent, hotel_country, hotel_market, hotel_cluster, srch_children_cnt,
        is_booking, user_location_country, srch_destination_id)

      return Option.apply(hotel)

    } catch {
      case e: Exception =>
        return None
    }

  def parseLine(line: String): Array[String] = {
     line.split(",", -1)


  }


}