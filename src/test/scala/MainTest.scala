package scala

import java.io.IOException

import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Before, Test}

class MainTest{


  @Before
  @throws[IllegalArgumentException]
  @throws[IOException]
  def init(): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  }

  @Test
  def clearKeyTest(): Unit = {
    Assert.assertEquals("sed", "sed")
  }



}
