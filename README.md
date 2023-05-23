# Data Quality Testing with Deequ in Spark

The sample repository shows how to use Deequ to perform data quality testing in Spark.
This repository is referenced in the blog post [Data Quality Testing with Deequ in AWS Glue](#todo).

## Usage

### Running on Glue

### Running on Glue Locally

This step assumes you have installed AWS Glue Library either locally or in a docker container using the steps
described in the [Developing and testing AWS Glue job scripts locally](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html).

Build and package the scala scripts

```bash
sbt compile && sbt package
```

If you want to run the job locally, you can use the following command:

```bash
gluesparksubmit \
          --jars ./target/libs/deequ-2.0.3-spark-3.3.jar \
          --class ExampleGlue \
          ./target/scala-2.12/glue-deequ_2.12-0.1.0.jar \
          --input_file_path="./data/iowa_liquor_sales_lite/year=2022/iowa_liquor_sales_01.csv" \
          --output_file_path="./outputs/iowa_liquor_sales_processed/data_01.parquet" \
          --metrics_path="./outputs/iowa_liquor_sales_processed/dataquality"
```

### Running on Spark

Build and package the scala scripts

```bash
sbt compile && sbt package
```

Run the job using

```bash
spark-submit \
          --jars ./target/libs/deequ-2.0.3-spark-3.3.jar \
          --class ExampleSpark \
          ./target/scala-2.12/glue-deequ_2.12-0.1.0.jar \
          "./data/iowa_liquor_sales_lite/year=2022/iowa_liquor_sales_01.csv" \
          "./outputs/iowa_liquor_sales_processed/data_01.parquet" \
          "./outputs/iowa_liquor_sales_processed/dataquality"
```