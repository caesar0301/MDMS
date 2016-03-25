name := "kalin"

version := "0.1.1"

scalaVersion := "2.10.4" // For Spark 1.3.0

conflictManager := ConflictManager.latestRevision

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.2.0" % "provided"

libraryDependencies += "joda-time" % "joda-time" % "2.6"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.5"

libraryDependencies += "org.yaml" % "snakeyaml" % "1.15"

// For testing

libraryDependencies += "log4j" % "log4j" % "1.2.17" % "test"

libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"

libraryDependencies += "org.specs2" %% "specs2-core" % "2.4.15" % "test"

libraryDependencies += "org.specs2" %% "specs2-junit" % "2.4.15" % "test"