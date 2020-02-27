//Class to create data frame

package sbtspark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession, _}
class FrameMaker {
  //Function to return data frame
  def toFrame(schema: StructType, data: List[Row]): DataFrame = {
    val appName = "FrameMaker"
    val master = "local"
    val spark = SparkSession.builder.appName(appName).master(master).getOrCreate()
    val rdd = spark.sparkContext.parallelize(data)
    val data_frame = spark.createDataFrame(rdd, schema)
    data_frame
  }
}
