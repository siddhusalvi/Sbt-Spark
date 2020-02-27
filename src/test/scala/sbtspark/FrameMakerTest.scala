package sbtspark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Test
import org.junit.Assert.assertEquals
import org.scalatest._

class FrameMakerTest {
  @Test def test1 {
    val data = List(
      Row("kartik", 90, "Ml"),
      Row("gaja", 85, "kotlin"),
      Row("mayank", 70, "automation")
    )

    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("performance", IntegerType, true),
        StructField("technology", StringType, true)
      )
    )
    val appName = "Scala example"
    val master = "local"
    val spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    val rdd = spark.sparkContext.parallelize(data)
    //Creating date frame by local session
    val expected_frame = spark.createDataFrame(rdd, schema)

    //getting data frame from FrameMaker class
    var frameMaker = new FrameMaker
    val frame1 = frameMaker.toFrame(schema, data)

    assertEquals(expected_frame,frame1)
  }

  @Test def test2 {
    val data = List(
      Row("kartik", 90, "Ml"),
      Row("gaja", 85, "kotlin"),
      Row("mayank", 70, "aautomation")
    )

    val data1 = List(
      Row("kartik", 90, "Ml"),
      Row("gaja", 85, "kotlin"),
      Row("mayank", 70, "automation")
    )

    val schema = StructType(
      List(
        StructField("name", StringType, true),
        StructField("performance", IntegerType, true),
        StructField("technology", StringType, true)
      )
    )
    val appName = "Scala example"
    val master = "local"
    val spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    val rdd = spark.sparkContext.parallelize(data)
    //Creating date frame by local session
    val expected_frame = spark.createDataFrame(rdd, schema)

    //getting data frame from FrameMaker class
    var frameMaker = new FrameMaker
    val frame1 = frameMaker.toFrame(schema, data1)
    assertEquals(expected_frame,frame1)
  }
}
