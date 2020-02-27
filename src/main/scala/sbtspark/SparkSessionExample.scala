package sbtspark

import org.apache.spark.sql.SparkSession

object SparkSessionExample {
  def main(args: Array[String]): Unit = {
    try {
      //Creating spark session
      val sparkSession = SparkSession.builder()
        .master("local")
        .appName("spark session example")
        .getOrCreate()

      val path = "/home/admin1/IdeaProjects/spark/src/main/scala/sbtspark/data.csv"
      val dataFrame = sparkSession.read
        .option("header", true)
        .csv(path)
      dataFrame.show()
    } catch {
      case _ => println("Exception occurred.")
    }
  }
}
