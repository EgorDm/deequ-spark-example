import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.analyzers._
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.schema.{RowLevelSchema, RowLevelSchemaValidator}
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.log.GlueLogger
import com.amazonaws.services.glue.util.{GlueArgParser, Job}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{functions => F}

import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object ExampleGlue {
  private val sparkContext: SparkContext = new SparkContext()
  private val glueContext: GlueContext = new GlueContext(sparkContext)
  private val spark = glueContext.getSparkSession
  private val logger = new GlueLogger()

  def main(sysArgs: Array[String]): Unit = {
    // Parse job arguments
    val args = GlueArgParser.getResolvedOptions(
      sysArgs,
      Seq("JOB_NAME", "input_file_path", "output_file_path", "metrics_path").toArray
    )

    // Initialize Glue job
    Job.init(args("JOB_NAME"), glueContext, args.asJava)

    // Read the CSV input file
    val rawDf = spark.read
      .option("header", "true")
      .csv(args("input_file_path"))

    // Perform schema validation
    val schema = RowLevelSchema()
      .withStringColumn("Invoice/Item Number", isNullable = false)
      .withStringColumn("Date", isNullable = false)
      .withStringColumn("Store Name", isNullable = false)
      .withStringColumn("Zip Code", isNullable = false)
      .withStringColumn("Vendor Name", isNullable = false)
      .withIntColumn("Item Number", isNullable = false)
      .withIntColumn("Bottles Sold", isNullable = false)
      .withDecimalColumn("Sale", isNullable = false, precision = 12, scale = 2)
      .withDecimalColumn("Volume Sold (Liters)", isNullable = true, precision = 12, scale = 2)
    val schemaResult = RowLevelSchemaValidator.validate(rawDf, schema)
    if (schemaResult.numInvalidRows > 0) {
      logger.error(
        s"Schema validation failed with ${schemaResult.numInvalidRows} invalid rows. Results: ${schemaResult}")
      schemaResult.invalidRows.show(10, truncate = false)
      sys.exit(1)
    }

    val validDf = schemaResult.validRows
      .withColumn("Zip Code", F.regexp_extract(F.col("Zip Code"), "[0-9]{5}", 0))

    // Profile the Dataset
    ConstraintSuggestionRunner()
      .onData(validDf)
      .useSparkSession(spark)
      .overwritePreviousFiles(true)
      .saveConstraintSuggestionsJsonToPath(s"${args("metrics_path")}/suggestions.json")
      .saveColumnProfilesJsonToPath(s"${args("metrics_path")}/profiles.json")
      .addConstraintRules(Rules.DEFAULT)
      .run()

    // Define Data Quality Checks
    val checks = Seq(
      Check(CheckLevel.Error, "Dataset checks")
        .hasSize(_ >= 0, Some("Dataset should not be empty")),
      Check(CheckLevel.Error, "Completeness checks")
        .isComplete("Invoice/Item Number")
        .isComplete("Date")
        .isComplete("Store Name")
        .isComplete("Zip Code")
        .isComplete("Vendor Name")
        .isComplete("Item Number")
        .isComplete("Bottles Sold")
        .isComplete("Sale"),
      Check(CheckLevel.Error, "Uniqueness checks")
        .isUnique("Invoice/Item Number"),
      Check(CheckLevel.Error, "Validity checks")
        .hasPattern("Invoice/Item Number", "^INV-[0-9]{11}$".r)
        .hasPattern("Date", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$".r)
        .hasPattern("Zip Code", "^[0-9]{5}$".r)
        .isNonNegative("`Bottles Sold`")
        .isNonNegative("`Sale`")
        .isNonNegative("`Volume Sold (Liters)`")
    )

    // Define analysers
    val analysers = (
      numericMetrics("Bottles Sold")
        ++ numericMetrics("Sale")
        ++ numericMetrics("Volume Sold (Liters)")
        ++ categoricalMetrics("Store Name")
        ++ categoricalMetrics("Vendor Name")
        ++ Seq(
          Completeness("Volume Sold (Liters)"),
        )
    )

    val verificationResult = VerificationSuite()
      .onData(validDf)
      .useSparkSession(spark)
      .overwritePreviousFiles(true)
      .saveCheckResultsJsonToPath(s"${args("metrics_path")}/checks.json")
      .saveSuccessMetricsJsonToPath(s"${args("metrics_path")}/metrics.json")
      .addChecks(checks)
      .addRequiredAnalyzers(analysers)
      .run()
    if (verificationResult.status == CheckStatus.Error) {
      logger.error(s"Data quality checks failed. Results: ${verificationResult.checkResults}")
      sys.exit(1)
    }

    // Write the result to S3 in Parquet format
    validDf.write
      .mode("overwrite")
      .parquet(args("output_file_path"))

    // Commit the job (for bookmarks)
    Job.commit()
  }

  private def numericMetrics(column: String): Seq[Analyzer[_, Metric[_]]] = {
    Seq(
      Minimum(column),
      Maximum(column),
      Mean(column),
      StandardDeviation(column),
      ApproxQuantile(column, 0.5)
    )
  }

  private def categoricalMetrics(column: String): Seq[Analyzer[_, Metric[_]]] = {
    Seq(
      CountDistinct(column),
    )
  }
}
