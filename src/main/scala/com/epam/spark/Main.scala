package com.epam.spark

import org.apache.spark.sql.DataFrame


object Main extends App {

  val sparkSession = SparkConfiguration.sparkSession
  var taskUtil = new TaskUtil
  var trainDF: DataFrame = taskUtil.createDataFrame(sparkSession, "src\\main\\resources\\train.csv")
  //trainDF.createOrReplaceTempView("train")
  //trainDF.show(5)


  taskUtil.task1(sparkSession, trainDF).show()
  taskUtil.task2(sparkSession, trainDF)
  taskUtil.task3(sparkSession, trainDF)


  sparkSession.close()

}
