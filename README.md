# Data Quality Testing with Deequ in Spark

The sample repository shows how to use Deequ to perform data quality testing in Spark.
This repository is referenced in the blog post [Data Quality Testing with Deequ in Spark](https://www.luminis.eu/blog/data-quality-testing-with-deequ-in-spark/).

## Setup

To begin, you need to have a working Scala development environment. If you don't, install
Java, [Scala](https://www.scala-lang.org/download/)
and [sbt (Scala Build Tool)](https://www.scala-sbt.org/download.html). For Linux x86 the installation would look as
follows:

```sh
# Install Java (on Debian)
sudo apt install default-jre

# Install Coursier (Scala Version Manager)
curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs && ./cs setup

# Install Scala 2.12 and sbt
cs install scala:2.12.15 && cs install scalac:2.12.15
```

Next, download a compatible [Apache Spark](https://spark.apache.org/downloads.html) distribution (version 3.3.x is
recommended) and add the `bin` folder to your system path. If you can run `spark-submit`, you are all set.

```sh
# Download Spark
curl https://dlcdn.apache.org/spark/spark-3.3.2/spark-3.3.2-bin-hadoop3.tgz --output hadoop.tgz
tar xvf hadoop.tgz
mv spark-3.3.2-bin-hadoop3 /usr/local/spark

# Add following line to your .bashrc (adds Spark to PATH)
export PATH="$PATH:/usr/local/spark/bin"
```

## Usage

### Starter Script

You will find
an [empty example Spark script](https://github.com/EgorDm/deequ-spark-example/blob/master/src/main/scala/EmptyExample.scala)
that reads a CSV file and writes it in parquet format to the output path. It takes the input path, output path and a
path for metric storage as command line arguments.

Compile the script with the following command, which will output the jar
as `target/scala-2.12/glue-deequ_2.12-0.1.0.jar`.

```shell
sbt compile && sbt package
```

Run the job using

```shell
spark-submit \
	--class EmptyExample \  
	./target/scala-2.12/glue-deequ_2.12-0.1.0.jar \  
	"./data/iowa_liquor_sales_lite/year=2022/iowa_liquor_sales_01.csv" \  
	"./outputs/sales/iowa_liquor_sales_processed" \  
	"./outputs/dataquality/iowa_liquor_sales_processed"
```

### Example Deequ Checks Script

Since we will be using the Deequ library, it must be added as a dependency to our project. While the library is already
included in the project's dependencies, it is deliberately not bundled into the compiled jar. Instead, you can use the
following command to extract it to the `target/libs` folder, or you can download it yourself from
the [maven repository](https://repo1.maven.org/maven2/com/amazon/deequ/deequ/).

```shell
sbt copyRuntimeDependencies
```

Pass the `--jars` option to Spark job, so the library is loaded at runtime:

```bash  
spark-submit \
	--jars ./target/libs/deequ-2.0.3-spark-3.3.jar \  
	--class ExampleSpark \  
	./target/scala-2.12/glue-deequ_2.12-0.1.0.jar \  
	"./data/iowa_liquor_sales_lite/year=2022/iowa_liquor_sales_01.csv" \  
	"./outputs/sales/iowa_liquor_sales_processed" \  
	"./outputs/dataquality/iowa_liquor_sales_processed"  
```

After running the command, the output parquet files are stored in `outputs/sales/iowa_liquor_sales_processed` and can be
inspected with pandas or data tools like [tad](https://duckdb.org/docs/guides/data_viewers/tad.html).
