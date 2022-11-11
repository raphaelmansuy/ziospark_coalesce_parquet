name := "ziospark-parquet-coalesce"

scalaVersion := "2.12.17"

val sparkVersion = "3.3.1"

libraryDependencies ++= Seq(
  "io.univalence" %% "zio-spark" % "0.8.1", // https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core" % sparkVersion, 
  "org.apache.spark" %% "spark-sql" % sparkVersion, 
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion
)
