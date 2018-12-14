package com.epam.spark

import org.apache.spark.sql.{DataFrame, SparkSession}


object Main extends App {
  val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
  var taskUtil = new TaskUtil
  var trainDF: DataFrame = taskUtil.createDataFrame(sparkSession, "src\\main\\resources\\train.csv")
  //trainDF.createOrReplaceTempView("train")
  //trainDF.show(5)


  taskUtil.task1(sparkSession, trainDF)
  taskUtil.task2(sparkSession, trainDF)
  taskUtil.task3(sparkSession, trainDF)


  sparkSession.close()

}
