ThisBuild / version := "1.0"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "lab01"
  )

libraryDependencies += "org.json4s" %% "json4s-jackson" % "4.0.6"