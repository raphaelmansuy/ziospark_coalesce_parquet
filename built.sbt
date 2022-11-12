name := "ziospark-parquet-coalesce"

scalaVersion := "2.12.17"

val sparkVersion = "3.3.1"
val zioVersion = "2.0.3"

libraryDependencies ++= Seq(
  "io.univalence" %% "zio-spark" % "0.8.1", // https://index.scala-lang.org/univalence/zio-spark/zio-spark
  "org.apache.spark" %% "spark-core" % sparkVersion, 
  "org.apache.spark" %% "spark-sql" % sparkVersion, 
  "org.apache.spark" %% "spark-hadoop-cloud" % sparkVersion,
  // import zio 
  "dev.zio" %% "zio" % zioVersion, // https://index.scala-lang.org/zio/zio/zio
  "dev.zio" %% "zio-test" % zioVersion % "test", // https://mvnrepository.com/artifact/dev.zio/zio-test
  "dev.zio" %% "zio-test-sbt" % zioVersion % "test", // https://mvnrepository.com/artifact/dev.zio/zio-test-sbt
)

testFrameworks := Seq(new TestFramework("zio.test.sbt.ZTestFramework"))