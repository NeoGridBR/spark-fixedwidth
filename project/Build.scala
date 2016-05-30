import sbt._

object MyBuild extends Build {

  lazy val root = Project("root", file(".")) dependsOn csvProj
  lazy val csvProj = RootProject(uri("https://github.com/quartethealth/spark-csv.git"))

}