package com.epam.spark


import org.apache.spark.sql.{SQLContext, SparkSession}


object Main extends App  {
  val spark = SparkSession.builder().master("local").getOrCreate()
  var trainDF = spark.read.format("csv").
    option("header", "true").
    option("delimiter", ",").
    load("src\\main\\resources\\train.csv")

  //trainDF.show(5)
  trainDF.createOrReplaceTempView("train")
  val sqlDF = spark.sql("SELECT  hotel_continent,hotel_country,hotel_market,count(*) as count \nFROM train" +
    "\nGROUP BY hotel_continent,hotel_country,hotel_market\nORDER BY count desc\nLIMIT 3")
  sqlDF.show()


}
