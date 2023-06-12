import com.amazon.deequ.{AnomalyCheckConfig, VerificationResult, VerificationSuite}
import com.amazon.deequ.analyzers._
import com.amazon.deequ.analyzers.runners.AnalysisRunner
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.metrics.Metric
import com.amazon.deequ.repository.{MetricsRepository, ResultKey}
import com.amazon.deequ.repository.fs.FileSystemMetricsRepository
import com.amazon.deequ.schema.{RowLevelSchema, RowLevelSchemaValidator}
import com.amazon.deequ.suggestions.{ConstraintSuggestionRunner, Rules}
import com.amazon.deequ.anomalydetection.RelativeRateOfChangeStrategy
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.{SparkSession, functions => F}

import java.io.{BufferedWriter, FileNotFoundException, OutputStreamWriter}

object ExampleSpark {
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

    // Perform schema validation
    val schema = RowLevelSchema()
      .withStringColumn("Invoice/Item Number", isNullable = false)
      .withStringColumn("Date", isNullable = false)
      .withStringColumn("Store Name", isNullable = false)
      .withStringColumn("Zip Code", isNullable = false)
      .withStringColumn("Vendor Name", isNullable = false)
      .withIntColumn("Item Number", isNullable = false)
      .withIntColumn("Bottles Sold", isNullable = true)
      .withDecimalColumn("Sale", isNullable = false, precision = 12, scale = 2)
      .withDecimalColumn("Volume Sold (Liters)", isNullable = false, precision = 12, scale = 2)
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
      Check(CheckLevel.Error, "Sales incremental checks")
        .hasSize(_ >= 0, Some("Dataset should not be empty"))
        .isComplete("Invoice/Item Number")
        .isComplete("Date")
        .isComplete("Store Name")
        .isComplete("Zip Code")
        .isComplete("Vendor Name")
        .isComplete("Item Number")
        .isComplete("Sale")
        .isComplete("Volume Sold (Liters)")
        .isUnique("Invoice/Item Number")
        .hasPattern("Invoice/Item Number", "^INV-[0-9]{11}$".r)
        .hasPattern("Date", "^[0-9]{4}-[0-9]{2}-[0-9]{2}$".r)
        .hasPattern("Zip Code", "^[0-9]{5}$".r)
        .isNonNegative("`Bottles Sold`")
        .isNonNegative("`Sale`")
        .isNonNegative("`Volume Sold (Liters)`")
    )

    val incrementalStateProvider = InMemoryStateProvider()

    // Initialize metrics repository for persistent metric storage
    val metricsRepository: MetricsRepository =
      FileSystemMetricsRepository(spark, s"${args("metrics_path")}/metrics_repository.json")
    val mergedMetricsRepository: MetricsRepository =
      FileSystemMetricsRepository(spark, s"${args("metrics_path")}/merged_metrics_repository.json")

    // Define analysers
    val analysers = (
      numericMetrics("Bottles Sold")
        ++ numericMetrics("Sale")
        ++ numericMetrics("Volume Sold (Liters)")
        ++ categoricalMetrics("Store Name")
        ++ categoricalMetrics("Vendor Name")
        ++ Seq(
          Completeness("Bottles Sold"),
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
      .useRepository(metricsRepository)
      .addAnomalyCheck(
        RelativeRateOfChangeStrategy(maxRateIncrease = Some(2.0)),
        Size(),
        Some(
          AnomalyCheckConfig(
            CheckLevel.Warning,
            "Dataset doubling or halving is anomalous"
          ))
      )
      .saveStatesWith(incrementalStateProvider)
      .run()
    if (verificationResult.status == CheckStatus.Error) {
      logger.error(s"Data quality checks failed. Results: ${verificationResult.checkResults}")
      sys.exit(1)
    }

    // Initialize state for incremental metric computation
    val completeStatePath = s"${args("metrics_path")}/state_repository/"
    val completeStateProvider = HdfsStateProvider(spark, s"${completeStatePath}/state.json", allowOverwrite = true)
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val aggregateStates = try {
      fs.listFiles(new Path(completeStatePath), false).hasNext
    } catch {
      case _: FileNotFoundException => false
    }

    // Merge incremental metrics with complete metrics, and run data quality checks
    val completeChecks = Seq(
      Check(CheckLevel.Error, "Sales complete checks")
        .hasSize(_ >= 0, Some("Dataset should not be empty"))
    )
    logger.info("Running complete dataset checks")
    val completeVerificationResult = VerificationSuite.runOnAggregatedStates(
      validDf.schema,
      completeChecks,
      if (aggregateStates) Seq(completeStateProvider, incrementalStateProvider)
      else Seq(incrementalStateProvider),
      saveStatesWith = Some(completeStateProvider)
    )

    // Store merged metrics in separate file.
    writeToTextFileOnDfs(spark, s"${args("metrics_path")}/complete_metrics.json", overwrite = true) { writer =>
      writer.append(VerificationResult.successMetricsAsJson(completeVerificationResult))
      writer.newLine()
    }

    if (completeVerificationResult.status == CheckStatus.Error) {
      logger.error(s"Complete data quality checks failed. Results: ${completeVerificationResult.checkResults}")
      sys.exit(1)
    }

    // Write the result in Parquet format
    validDf.write
      .mode("append")
      .parquet(args("output_file_path"))
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

  private def writeToTextFileOnDfs(session: SparkSession, path: String, overwrite: Boolean = false)(
    writeFunc: BufferedWriter => Unit): Unit = {

    val (fs, qualifiedPath) = asQualifiedPath(session, path)
    val output = fs.create(qualifiedPath, overwrite)

    try {
      val writer = new BufferedWriter(new OutputStreamWriter(output))
      writeFunc(writer)
      writer.close()
    } finally {
      if (output != null) {
        output.close()
      }
    }
  }

  private def asQualifiedPath(session: SparkSession, path: String): (FileSystem, Path) = {
    val hdfsPath = new Path(path)
    val fs = hdfsPath.getFileSystem(session.sparkContext.hadoopConfiguration)
    val qualifiedPath = hdfsPath.makeQualified(fs.getUri, fs.getWorkingDirectory)

    (fs, qualifiedPath)
  }
}
