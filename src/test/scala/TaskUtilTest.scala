import java.util

import com.epam.spark.TaskUtil
import com.epam.spark.SparkConfiguration
import org.apache.spark.sql.{DataFrame, Row}

import org.junit.{Assert, Test}

class TaskUtilTest {

  @Test
  def task1Test(): Unit = {
    val expected: String = "[[2,50,628,85501], [2,50,675,63024], [2,50,365,47175]]"
    val expectedCount: Long = 3

    val frame: DataFrame = new TaskUtil().createDataFrame(SparkConfiguration.sparkSession, "src/test/resources/test.csv")
    val actual: util.List[Row] = new TaskUtil().task1(SparkConfiguration.sparkSession, frame).collectAsList()


    Assert.assertEquals(expected, actual.toString)
    Assert.assertEquals(expectedCount, actual.size())
  }


  @Test
  def dataFrameNotNull(): Unit = {
    Assert.assertNotNull(new TaskUtil().createDataFrame(SparkConfiguration.sparkSession, "src/test/resources/test.csv"))
  }

  @Test(expected = classOf[org.apache.spark.sql.AnalysisException])
  def wrongInputPathTest(): Unit = {
    val actual: DataFrame = new TaskUtil().createDataFrame(SparkConfiguration.sparkSession, "hvghfg")

  }

  @Test(expected = classOf[java.lang.IllegalArgumentException])
  def nullInputPathTest(): Unit = {
   val actual: DataFrame = new TaskUtil().createDataFrame(SparkConfiguration.sparkSession, "")

  }

}
