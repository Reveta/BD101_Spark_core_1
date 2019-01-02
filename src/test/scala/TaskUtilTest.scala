import com.epam.spark.{Hotel, SparkConfiguration, TaskUtil}
import org.apache.spark.rdd.RDD
import org.junit.{Assert, Test}

class TaskUtilTest {

  @Test
  def task1Test(): Unit = {
    val expected: String = "[[2,50,628,85501], [2,50,675,63024], [2,50,365,47175]]"
    val expectedCount: Long = 3

    val rdd: RDD[Hotel] = new TaskUtil().createRDD(SparkConfiguration.sparkSession, "src/test/resources/test.csv")
    val actual: Array[((String, String, String), Int)] = new TaskUtil().task1(rdd)


    Assert.assertEquals(expected, actual.toString)
    Assert.assertEquals(expectedCount, actual.length)
  }


  @Test
  def dataFrameNotNull(): Unit = {
    Assert.assertNotNull(new TaskUtil().createRDD(SparkConfiguration.sparkSession, "src/test/resources/test.csv"))
  }

  @Test(expected = classOf[org.apache.spark.sql.AnalysisException])
  def wrongInputPathTest(): Unit = {
    val actual: RDD[Hotel]  = new TaskUtil().createRDD(SparkConfiguration.sparkSession, "hvghfg")

  }

  @Test(expected = classOf[java.lang.IllegalArgumentException])
  def nullInputPathTest(): Unit = {
   val actual: RDD[Hotel]  = new TaskUtil().createRDD(SparkConfiguration.sparkSession, "")

  }

}
