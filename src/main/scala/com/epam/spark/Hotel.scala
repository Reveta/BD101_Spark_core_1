package com.epam.spark


case class Hotel(
                  //TODO Проінспектувати індекси на валідність даних

                  srch_adults_cnt: String,
                  hotel_continent: String,
                  hotel_country: String,
                  hotel_market: String,
                  hotel_cluster: String,
                  srch_children_cnt: String,
                  is_booking: String,
                  user_location_country: String,
                  srch_destination_id: String

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

  def createHotel(metadata: Array[String]): Hotel =
    try {
    val srch_adults_cnt: String = metadata(ADULTS)
    val hotel_continent: String = metadata(HOTEL_CONTINENT)
    val hotel_country: String = metadata(HOTEL_COUNTRY)
    val hotel_market: String = metadata(HOTEL_MARKET)
    val hotel_cluster: String = metadata(HOTEL_CLUSTER)
    val srch_children_cnt: String = metadata(CHILDREN)
    val is_booking: String = metadata(IS_BOOKING)
    val user_location_country: String = metadata(USER_LOCATION_COUNTRY)
    val srch_destination_id: String = metadata(DESTINATION_ID)

    val hotel = new Hotel(srch_adults_cnt, hotel_continent, hotel_country, hotel_market, hotel_cluster, srch_children_cnt,
      is_booking, user_location_country, srch_destination_id)

    return hotel

  } catch {
    case e: Exception =>
      return null
  }
}