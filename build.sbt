name := "spark-fixedwidth"

version := "1.1"

organization := "com.quartethealth"

lazy val credentialsPath = Path.userHome / ".sbt" / ".credentials"
publishTo := Some("Artifactory Realm" at "http://repo.neogrid.com/libs-sbt-release")
credentials += Credentials(credentialsPath)

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.univocity" % "univocity-parsers" % "1.5.1",
  "org.apache.spark" %% "spark-sql" % "1.6.0" % "provided",
  "org.apache.spark" %% "spark-core" % "1.6.0" % "provided",
  "com.databricks" %% "spark-csv" % "1.4.0" % "provided",


  "org.scalatest" %% "scalatest" % "2.2.1" % "test",
  "org.specs2" %% "specs2-core" % "3.7" % "test"
)

scalacOptions in Test ++= Seq("-Yrangepos")
