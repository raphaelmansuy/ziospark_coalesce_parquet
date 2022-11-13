package elitizon.ziospark.coalesce

import org.apache.spark.sql.Row

import zio._
import zio.spark.experimental
import zio.spark.experimental.Pipeline
import zio.spark.parameter._
import zio.spark.sql._
import zio.spark.sql.implicits._
import org.apache.log4j.{Level, Logger}

import org.apache.spark.storage.StorageLevel

// A cli app that reads a parquet file, coalesce it and write it to a new parquet file
object App extends ZIOAppDefault {

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

//  val pathInputParquet =
//    "/Users/raphaelmansuy/Downloads/dkt_tables/bronze/store_item_follow"
  val pathInputParquet =
    "s3a://ookla-open-data/parquet/performance/type=fixed/year=2022"

//  val pathInputParquet =
//    "s3a://ookla-open-data/parquet/performance/" // "/Users/raphaelmansuy/Downloads/input_parquet/"

  // val pathInputParquet = "/Users/raphaelmansuy/Downloads/input_parquet/"
  val pathOutputParquet = "/Users/raphaelmansuy/Downloads/output_parquet/"

  // Votre programme
  def program: SIO[Unit] = for {

    _ <- ZIO.log("🚀 🚀 🚀 Application started")

    inputDataFrame <- DataFrameUtil.readParquetAndPersist(pathInputParquet)

    estimatedSize <- DataFrameUtil.estimatedSizeDataFrame(inputDataFrame)
    numPartitions <- ZIO
      .succeed(estimatedSize / DataFrameUtil.blockSize128)
      .map(_.toInt)

    estimatedSizedStr <- ZIO.succeed(FormatUtil.formatSize(estimatedSize))

    _ <- ZIO.log(s"✅ estimatedSize: $estimatedSizedStr")
    _ <- ZIO.log(s"✅ numPartitions: $numPartitions 🔥")

    _ <- ZIO.log(
      s"🥳 saving DataFrame $pathInputParquet with numPartitions: $numPartitions"
    )

    _ <-
      DataFrameUtil
        .writeParquet(
          inputDataFrame.repartition(numPartitions),
          pathOutputParquet,
          DataFrameUtil.blockSize128
        )

    _ <- ZIO.log(s"🥳 End saving with numPartitions: $numPartitions")

    _ <- ZIO.log("🚀 🚀 🚀 Application finished")

  } yield ()

  private val session: ZLayer[Any, Throwable, SparkSession] =
    SparkSessionLayer.session

  override def run: ZIO[ZIOAppArgs, Any, Any] = program.provide(session)
}
