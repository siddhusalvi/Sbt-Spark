package sbtspark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
object DataFrameOperation extends App {

  val appName = "Scala example"
  val master = "local"

  val spark = SparkSession.builder.appName(appName).master(master).getOrCreate()

  val data = List(
    Row("kartik",90,"Ml"),
    Row("gaja",85,"kotlin"),
    Row("mayank",70,"automation")
  )

  val schema = StructType(
    List(
      StructField("name",StringType,true),
      StructField("performance",IntegerType,true),
      StructField("technology",StringType,true)
    )
  )
  val rdd = spark.sparkContext.parallelize(data)
  val df = spark.createDataFrame(rdd,schema)
  println(df.schema)
  df.show()
}
