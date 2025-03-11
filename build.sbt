ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

mainClass in (Compile, run) := Some("com.etl.UserEventETL")

lazy val root = (project in file("."))
  .settings(
    name := "WAU"
  )
