package com.epam.spark

import org.apache.spark.sql.SparkSession

object SparkConfiguration {
  /**
    * Creates a spark session.
    */
  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()
}
