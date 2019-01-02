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
                  srch_destination_id: Int) {

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

  def apply(row: String): Hotel = {
    try {
//      print("[INFO] Start creating Hotel - " + row + "\n")

      val rowArray: Array[String] = row.split(",", -1)
//      if (rowArray.length != 24) { println(Console.GREEN + "[ERROR] " + "rowArray.length != 24" + " - " + row + Console.RESET)}

      val hotel: Hotel = new Hotel(
        Integer.valueOf(rowArray(ADULTS)),
        Integer.valueOf(rowArray(HOTEL_CONTINENT)),
        Integer.valueOf(rowArray(HOTEL_COUNTRY)),
        Integer.valueOf(rowArray(HOTEL_MARKET)),
        Integer.valueOf(rowArray(HOTEL_CLUSTER)),
        Integer.valueOf(rowArray(CHILDREN)),
        Integer.valueOf(rowArray(IS_BOOKING)),
        Integer.valueOf(rowArray(USER_LOCATION_COUNTRY)),
        Integer.valueOf(rowArray(DESTINATION_ID)))

      return hotel
    } catch {
      case e: Exception => /*println(Console.GREEN + "[ERROR] " + e.toString + " - " + row + Console.RESET)*/
        return null
    }
  }


}