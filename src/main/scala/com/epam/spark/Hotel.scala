package com.epam.spark

/** case calss for data from csv */
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
  /** Indexes from schema of csv data file */
  val ADULTS = 13
  val HOTEL_CONTINENT = 20
  val HOTEL_COUNTRY = 21
  val HOTEL_MARKET = 22
  val HOTEL_CLUSTER = 23
  val CHILDREN = 14
  val IS_BOOKING = 18
  val USER_LOCATION_COUNTRY = 3
  val DESTINATION_ID = 16

  /** Check is this line header of scv file */
  def isHeaderCsv(line: String): Boolean = line.startsWith("date_time,site_name")

  /** Constructor of Hotel class
    *
    * @param row from scv file
    * @return new Optional[Hotel]
    * @return if row is not valid - return None */
  def apply(row: String): Option[Hotel] = try {
    print("[INFO] Start creating Hotel - " + row + "\n")

    val rowArray: Array[String] = splitLine(row)
    if (rowArray.length != 24) {
      println(Console.GREEN + "[ERROR] " + "rowArray.length != 24" + " - " + row + Console.RESET)
      throw new Exception("rowArray.length != 24")
    }

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

    return Option.apply(hotel)
  } catch {
    case e: Exception => println(Console.GREEN + "[ERROR] " + e.toString + " - " + row + Console.RESET)
      return None
  }

  /** parse csv file by column */
  def splitLine(line: String): Array[String] = {
    return line.split(",", -1)
  }
}