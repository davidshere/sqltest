ThisBuild / scalaVersion := "2.13.6"
ThisBuild / organization := "org.davidshere"

lazy val hello = (project in file("."))
  .settings(
    name := "Hello",

    libraryDependencies ++= Seq(
      // Test dependencies
      "org.scalatest"   %% "scalatest"          % "3.2.7" % Test,

      // Connecting to a database
      "org.scalikejdbc" %% "scalikejdbc"        % "3.5.+",
      "org.postgresql"  %  "postgresql"         % "42.2.23",
      "ch.qos.logback"  %  "logback-classic"    % "1.2.+",

      // Reading CSV and YAML
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-csv" % "2.12.4",
      "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.12.4",
    ),

  )	 
