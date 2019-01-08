import com.epam.spark.{Hotel, SparkConfiguration, TaskUtil}
import org.apache.spark.rdd.RDD
import org.junit.{Assert, Test}

class TaskUtilTest {

  @Test
  def task1Test(): Unit = {
    val expected: String = "()"
    val expectedCount: Long = 0

    val rdd: RDD[Option[Hotel]] = TaskUtil.createRDD(SparkConfiguration.sparkSession, "src/test/resources/test.csv")
    val actual: Array[((Int, Int, Int), Int)] = TaskUtil.task1(rdd)
    val sb: StringBuilder = StringBuilder.newBuilder

    Assert.assertEquals(expected, actual.foreach(sb.append).toString)
    Assert.assertEquals(expectedCount, actual.length)
  }

  @Test
  def task2Test(): Unit = {
    val expected: String = "()"
    val expectedCount: Long = 0

    val rdd: RDD[Option[Hotel]] = TaskUtil.createRDD(SparkConfiguration.sparkSession, "src/test/resources/test.csv")
    val actual: Array[(Int, Int)] = TaskUtil.task2(rdd)
    val sb: StringBuilder = StringBuilder.newBuilder

    Assert.assertEquals(expected, actual.foreach(sb.append).toString)
    Assert.assertEquals(expectedCount, actual.length)
  }

  @Test
  def task3Test(): Unit = {
    val expected: String = "()"
    val expectedCount: Long = 0

    val rdd: RDD[Option[Hotel]] = TaskUtil.createRDD(SparkConfiguration.sparkSession, "src/test/resources/test.csv")
    val actual: Array[((Int, Int, Int), Int)] = TaskUtil.task3(rdd)
    val sb: StringBuilder = StringBuilder.newBuilder

    Assert.assertEquals(expected, actual.foreach(sb.append).toString)
    Assert.assertEquals(expectedCount, actual.length)
  }

  @Test
  def createTest(): Unit = {
    val input: String = "2014-09-30 11:21:02,2,3,66,332,55121,72.1107,32708,0,1,9,2014-09-30,2014-10-02,2,0,1,576,3,1,1,2,50,487,91"
    val expected = new Hotel(2, 2, 50, 487, 91,
      0, 1, 66, 576)
    val actual: Option[Hotel]= Hotel.createHotel(input)
    Assert.assertEquals(expected, actual.get)
  }

  @Test
  def parseTest(): Unit = {
    val input: String = "2014-09-30 11:21:02,2,3,66,332,55121,72.1107,32708,0,1,9,2014-09-30,2014-10-02,2,0,1,576,3,1,1,2,50,487,91"
    val expected :Array[String]= Array("2014-09-30 11:21:02","2","3","66","332","55121","72.1107","32708","0","1","9","2014-09-30",
      "2014-10-02","2","0","1","576","3","1","1","2","50","487","91")
    val actual: Array[String] = Hotel.parseLine(input)
    Assert.assertTrue(java.util.Arrays.equals(expected.asInstanceOf[Array[AnyRef]], actual.asInstanceOf[Array[AnyRef]]))
  }

  @Test
  def parseNullTest(): Unit = {
    val input: String = "2014dfgzsfgr-09-30 11:21:02,2,3,6fgs6,332,55121,72.1107,32708,0,1,9,2014-09-30,2014-10-02,2,0,1,576,3,1,1,2,50,487,91"
    val expected = None
    val actual: Option[Hotel] = Hotel.createHotel(input)
    Assert.assertEquals(expected, actual)
  }

  @Test
  def rddNotNull(): Unit = {
    Assert.assertNotNull(TaskUtil.createRDD(SparkConfiguration.sparkSession, "src/test/resources/test.csv"))
  }

  @Test(expected = classOf[org.apache.hadoop.mapred.InvalidInputException])
  def wrongInputPathTest(): Unit = {
    val actual: RDD[Option[Hotel]] = TaskUtil.createRDD(SparkConfiguration.sparkSession, "hvghfg")

  }

  @Test(expected = classOf[java.lang.IllegalArgumentException])
  def nullInputPathTest(): Unit = {
    val actual: RDD[Option[Hotel]] = TaskUtil.createRDD(SparkConfiguration.sparkSession, "")

  }

}
