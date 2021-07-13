ThisBuild / scalaVersion := "2.13.6"
ThisBuild / organization := "org.davidshere"

lazy val hello = (project in file("."))
  .settings(
    name := "Hello",

    libraryDependencies ++= Seq(
      "org.scalatest"   %% "scalatest"          % "3.2.7" % Test,
      "org.scalikejdbc" %% "scalikejdbc"        % "3.5.+",
      "org.postgresql"  %  "postgresql"         % "42.2.23",
      "ch.qos.logback"  %  "logback-classic"    % "1.2.+"
    ),

  )	 
