
ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.10"

lazy val root = (project in file("."))
  .settings(
    name := "feature"
  )

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.7"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.7"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.7"