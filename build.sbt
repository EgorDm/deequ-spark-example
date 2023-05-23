val projectName = "glue-deequ"
val glueVersion = "4.0.0"
val scalaCompatVersion = "2.12"
val scalaVersion_ = s"$scalaCompatVersion.7"

ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := scalaVersion_

lazy val copyRuntimeDependencies = taskKey[Unit]("Copy required jars to the lib folder")

lazy val root = (project in file("."))
  .settings(
    name := "glue-deequ",
    libraryDependencies ++= dependencies,
    resolvers ++=
      Resolver.sonatypeOssRepos("releases") ++ Seq(
        "aws-glue-etl-artifacts" at "https://aws-glue-etl-artifacts.s3.amazonaws.com/release/",
      ),
    copyRuntimeDependencies := {
      val outputFolder = new File("target/libs")

      // Copy dependencies
      (Compile / managedClasspath).value.files
        .filter(_.getName.contains("deequ"))
        .foreach { f =>
          println(s"Copying ${f.getName}")
          IO.copyFile(f, outputFolder / f.getName, CopyOptions().withOverwrite(false))
        }
    }
  )

lazy val dependencies = Seq(
  "com.amazon.deequ" % "deequ" % "2.0.3-spark-3.3" % Provided,
  "com.amazonaws" % "AWSGlueETL" % glueVersion % Provided,
)
