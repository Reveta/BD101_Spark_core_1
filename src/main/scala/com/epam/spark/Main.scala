package com.epam.spark


import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


object Main extends App {
  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()
  var trainDF: DataFrame = spark.read.format("csv").
    option("header", "true").
    option("delimiter", ",").
    load("src\\main\\resources\\train.csv")
  //----------------------------------------------------------task1--------------------------------------------------------------------------------------------------------
  //trainDF.show(5)
  //  trainDF.createOrReplaceTempView("train")
  //  val sqlDF: DataFrame = spark.sql("SELECT  hotel_continent,hotel_country,hotel_market,count(*) as count \nFROM train" +
  //    "\nGROUP BY hotel_continent,hotel_country,hotel_market\nORDER BY count desc\nLIMIT 3")
  //  sqlDF.show()

  //  trainDF.groupBy("hotel_continent","hotel_country","hotel_market" ).
  //    count()
  //    .orderBy(org.apache.spark.sql.functions.col("count").desc).limit(3)
  //    .show()


  //----------------------------------------------------------task2--------------------------------------------------------------------------------------------------------


  val sqlDF: DataFrame = spark.sql("SELECT  hotel_country, count(*) as count\n  FROM train\n   " +
    " WHERE user_location_country=srch_destination_id\n  GROUP BY hotel_country\n  ORDER BY count desc\n    limit 1")
  sqlDF.show()

}
