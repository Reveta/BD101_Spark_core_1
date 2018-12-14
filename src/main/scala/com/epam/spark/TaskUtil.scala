package com.epam.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

class TaskUtil {
  def createDataFrame(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read.format("csv").
      option("header", "true").
      option("delimiter", ",").
      load(path)
  }

  def task1(sparkSession: SparkSession, dataFrame: DataFrame): Unit = {

    //  val sqlDF: DataFrame = spark.sql("SELECT  hotel_continent,hotel_country,hotel_market,count(*) as count \nFROM train" +
    //    "\nGROUP BY hotel_continent,hotel_country,hotel_market\nORDER BY count desc\nLIMIT 3")
    //  sqlDF.show()

    dataFrame.groupBy("hotel_continent", "hotel_country", "hotel_market").
      count()
      .orderBy(org.apache.spark.sql.functions.col("count").desc).limit(3)
      .show()

  }


  def task2(sparkSession: SparkSession, dataFrame: DataFrame): Unit = {
    //  val sqlDF: DataFrame = spark.sql("SELECT  hotel_country, count(*) as count\n  FROM train\n   " +
    //    " WHERE user_location_country=srch_destination_id\n  GROUP BY hotel_country\n  ORDER BY count desc\n    limit 1")
    //  sqlDF.show()

    dataFrame.where("user_location_country=srch_destination_id").groupBy("hotel_country").
      count()
      .orderBy(org.apache.spark.sql.functions.col("count").desc).limit(1)
      .show()

  }

  def task3(sparkSession: SparkSession, dataFrame: DataFrame): Unit = {
    //    val sqlDF: DataFrame = spark.sql("SELECT  hotel_continent,hotel_country,hotel_market, count(*) as count\n " +
    //      " FROM train\n    WHERE srch_children_cnt>0 AND is_booking=0\n\n  GROUP BY hotel_continent,hotel_country,hotel_market\n " +
    //      " ORDER BY count desc\n    limit 3")
    //    sqlDF.show()

    dataFrame.where("srch_children_cnt>0 AND is_booking=0").groupBy("hotel_continent", "hotel_country", "hotel_market").
      count()
      .orderBy(org.apache.spark.sql.functions.col("count").desc).limit(3)
      .show()

  }
}
