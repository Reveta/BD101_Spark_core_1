import com.epam.spark.{Hotel, SparkConfiguration, Engine}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.{Assert, Test}

class EngineTest {

  val spark: SparkSession = SparkConfiguration.sparkSession


  @Test
  def IsRddCorrect(): Unit = {
    val rdd: RDD[Hotel] = Engine.createHotelsRDD("src/test/resources/train.csv")


    Assert.assertNotNull(rdd)
  }

  @Test/*(expected = IllegalArgumentException)*/
  def wrongCSVPathTest(): Unit = {
    val actual: RDD[Hotel]  = Engine.createHotelsRDD(" ")
  }

  @Test
  def task1Test(): Unit = {
    val expected: String = "((2,50,368),110)((6,105,29),99)((6,105,35),82)"
    val expectedCount: Long = 3

    val rdd: RDD[Hotel] = Engine.createHotelsRDD("src/test/resources/train.csv")
    val actual: Array[((Int, Int, Int), Int)] = Engine.task1(rdd)

    val sb = StringBuilder.newBuilder
    actual.foreach(sb.append)


    Assert.assertEquals(expected, sb.toString())
    Assert.assertEquals(expectedCount, actual.length)
  }

  @Test
  def task2Test(): Unit = {
    val expected: String = "(0,0)"

    val rdd: RDD[Hotel] = Engine.createHotelsRDD("src/test/resources/train.csv")
    val actual: (Int, Int) = Engine.task2(rdd)


    Assert.assertEquals(expected, actual.toString())
  }

  @Test
  def task3Test(): Unit = {
    val expected: String = "((2,50,368),119)((2,50,365),66)((2,50,366),46)"
    val expectedCount: Long = 3

    val rdd: RDD[Hotel] = Engine.createHotelsRDD("src/test/resources/train.csv")
    val actual: Array[((Int, Int, Int), Int)] = Engine.task3(rdd)

    val sb = StringBuilder.newBuilder
    actual.foreach(sb.append)


    Assert.assertEquals(expected, sb.toString())
    Assert.assertEquals(expectedCount, actual.length)
  }
}
