ThisBuild / scalaVersion := "2.13.6"
ThisBuild / organization := "org.davidshere"

lazy val sqlTest = (project in file("."))
  .settings(
    name := "SqlTest",

    libraryDependencies ++= Seq(
      // Test dependencies
      "org.scalatest"   %% "scalatest"          % "3.2.7" % Test,

      // Connecting to a database
      "org.postgresql"  %  "postgresql"         % "42.2.23",
      "ru.yandex.clickhouse" % "clickhouse-jdbc" % "0.3.1-patch",


      // Reading CSV and YAML
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.12.4",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.12.4",

      // SQL Parsing
      "com.github.jsqlparser" % "jsqlparser" % "4.1",
    ),

  )	 
