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

//  val pathInputParquet =  "/Users/raphaelmansuy/Downloads/dkt_tables/bronze/store_item_follow"
  // val pathInputParquet =
  //  "s3a://ookla-open-data/parquet/performance/type=fixed/year=2022" // "/Users/raphaelmansuy/Downloads/input_parquet/"

  val pathInputParquet =
    "s3a://ookla-open-data/parquet/performance/" // "/Users/raphaelmansuy/Downloads/input_parquet/"

  val pathOutputParquet = "/Users/raphaelmansuy/Downloads/output_parquet/"
  val pathOutputParquetRepartition =
    "/Users/raphaelmansuy/Downloads/output_parquet_repartition/"

  // block size 128MB
  val blockSize128 = 128 * 1024 * 1024
  // block size 64MB
  val blockSize64 = 64 * 1024 * 1024

  def read: SIO[DataFrame] = DataFrameUtil.readParquet(pathInputParquet)

  def readAndPersist: SIO[DataFrame] =
    DataFrameUtil.readParquetAndPersist(pathInputParquet)

  val pipelineCalculateNbRows =
    Pipeline(
      read,
      DataFrameUtil.identityTransform,
      DataFrameUtil.calculateNbRows
    )

  val pipelineEstimatedSizeDataFrame =
    Pipeline(
      read,
      DataFrameUtil.identityTransform,
      DataFrameUtil.estimatedSizeDataFrame
    )

  def calculateNbRows: SIO[Unit] = for {
    _ <- ZIO.log("Calculating number of rows")
    nbRows <- pipelineCalculateNbRows.run
    _ <- ZIO.log(s"✅ nbRows: ${nbRows} 🔥")
  } yield ()

  // format size in MB, GB, TB depending on the size
  // with separator between thousands
  def formatSize(size: Long): String = {
    if (size < 1024) {
      formatNumber(size) + " B"
    } else if (size < 1024 * 1024) {
      formatNumber(size / 1024) + " KB"
    } else if (size < 1024 * 1024 * 1024) {
      formatNumber(size / (1024 * 1024)) + " MB"
    } else {
      formatNumber(size / (1024 * 1024 * 1024)) + " GB"
    }
  }

  // format numeric with separator every 3 digits
  def formatNumber(number: Long): String = {
    val formatter = java.text.NumberFormat.getIntegerInstance
    formatter.format(number)
  }

  // Votre programme
  def program: SIO[Unit] = for {

    _ <- ZIO.log("🚀 🚀 🚀 Application started")

    estimatedSize <- pipelineEstimatedSizeDataFrame.run
    numPartitions <- ZIO.succeed(estimatedSize / blockSize64).map(_.toInt)

    _ <- ZIO.log(s"🥳 Running pipeline with numPartitions: $numPartitions")
    _ <- Pipeline(
      read, // Read
      { df => DataFrameUtil.repartition(df, numPartitions) }, // Transformaton
      { df => DataFrameUtil.writeParquet(df, pathOutputParquet) } // Action
    ).run
    _ <- ZIO.log(s"🥳 End pipeline with numPartitions: $numPartitions")

    _ <- ZIO.log(s"✅ estimatedSize: ${formatSize(estimatedSize)}")
    _ <- ZIO.log(s"✅ numPartitions: $numPartitions 🔥")

    _ <- ZIO.log("🚀 🚀 🚀 Application finished")

  } yield ()

  private val session: ZLayer[Any, Throwable, SparkSession] =
    SparkSessionLayer.session

  override def run: ZIO[ZIOAppArgs, Any, Any] = program.provide(session)
}
