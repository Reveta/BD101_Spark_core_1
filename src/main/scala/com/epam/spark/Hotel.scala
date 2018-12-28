package com.epam.spark

import org.apache.spark.sql.SparkSession


class Hotel(sp: SparkSession, srch_adults_cntI: String, hotel_continentI: String, hotel_countryI: String, hotel_marketI: String, hotel_clusterI: String) extends Serializable{
  private var spark: SparkSession = sp

  var srch_adults_cnt: String = srch_adults_cntI
  var hotel_continent: String = hotel_continentI
  var hotel_country: String = hotel_countryI
  var hotel_market: String = hotel_marketI
  var hotel_cluster: String = hotel_clusterI


  override def toString = s"Hotel($srch_adults_cnt, $hotel_continent, $hotel_country, $hotel_market, $hotel_cluster)"

}
