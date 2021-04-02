import Dependencies._

ThisBuild / scalaVersion     := "2.12.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "my-spark-ml-utils",
    libraryDependencies += scalaTest % Test
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0" % "provided"

// libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.0" 