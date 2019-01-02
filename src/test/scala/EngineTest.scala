import com.epam.spark.{Hotel, SparkConfiguration, Engine}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class EngineTest {

  val spark: SparkSession = SparkConfiguration.sparkSession


  def task1Test(): Unit = {
    val expected: String = "[[2,50,628,85501], [2,50,675,63024], [2,50,365,47175]]"
    val expectedCount: Long = 3

    val rdd: RDD[Hotel] = Engine.createHotelsRDD("src/test/resources/test.csv")
    val actual: Array[((Int, Int, Int), Int)] = Engine.task1(rdd)


    Assert.assertEquals(expected, actual.toString)
    Assert.assertEquals(expectedCount, actual.length)
  }

  @Test
  def dataFrameNotNull(): Unit = {
    Assert.assertNotNull(Engine.createHotelsRDD("src/test/resources/test.csv"))
  }


  @Test(expected = classOf[org.apache.spark.sql.AnalysisException])
  def wrongInputPathTest(): Unit = {
    val actual: RDD[Hotel]  = Engine.createHotelsRDD("hvghfg")
  }


  @Test(expected = classOf[java.lang.IllegalArgumentException])
  def nullInputPathTest(): Unit = {
    val actual: RDD[Hotel]  = Engine.createHotelsRDD("")
  }

}
