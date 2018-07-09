name := "spark-fixedwidth"
version := "1.2"
organization := "com.quartethealth"

lazy val credentialsPath = Path.userHome / ".sbt" / ".credentials"
lazy val sparkVersion =  "2.3.0"
publishTo := Some("Artifactory Realm" at "http://repo.neogrid.com/libs-sbt-release")
credentials += Credentials(credentialsPath)

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "com.univocity" % "univocity-parsers" % "1.5.1",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.commons" % "commons-csv" % "1.4",

  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.specs2" %% "specs2-core" % "3.7" % "test"
)

scalacOptions in Test ++= Seq("-Yrangepos")
