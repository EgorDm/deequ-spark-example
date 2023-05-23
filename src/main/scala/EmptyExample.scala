import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession

object EmptyExample {
  private val spark = SparkSession.builder.getOrCreate()
  private val logger = LogManager.getLogger()

  def main(sysArgs: Array[String]): Unit = {
    // Parse job arguments
    val args = Map(
      "input_file_path" -> sysArgs(0),
      "output_file_path" -> sysArgs(1),
      "metrics_path" -> sysArgs(2)
    )

    // Read the CSV input file
    val rawDf = spark.read
      .option("header", "true")
      .csv(args("input_file_path"))

    logger.info(s"Do some preprocessing")

    // Write the result to S3 in Parquet format
    rawDf.write
      .mode("overwrite")
      .parquet(args("output_file_path"))
  }
}
