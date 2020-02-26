
name := "spark"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-M2" % Test

libraryDependencies += "com.novocode" % "junit-interface" % "0.8" % "test->default"
