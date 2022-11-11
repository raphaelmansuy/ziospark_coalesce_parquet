name         := "simple-app"
scalaVersion := "2.12.17"

libraryDependencies ++= Seq(
  "io.univalence"    %% "zio-spark"  % "0.8.1", //https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core" % "3.3.1", // % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.3.1", //% "provided",
)
