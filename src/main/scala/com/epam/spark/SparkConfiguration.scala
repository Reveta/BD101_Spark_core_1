package com.epam.spark

import org.apache.spark.sql.SparkSession

object SparkConfiguration {
  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local")
    .getOrCreate()
}
