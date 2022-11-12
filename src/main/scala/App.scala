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

object App extends ZIOAppDefault {

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

//  val pathInputParquet =  "/Users/raphaelmansuy/Downloads/dkt_tables/bronze/store_item_follow"
  val pathInputParquet =
    "s3a://ookla-open-data/parquet/performance/type=fixed/year=2022" // "/Users/raphaelmansuy/Downloads/input_parquet/"
  val pathOutputParquet = "/Users/raphaelmansuy/Downloads/output_parquet/"
  val pathOutputParquetRepartition =
    "/Users/raphaelmansuy/Downloads/output_parquet_repartition/"

  // block size 128MB
  val blockSize128 = 128 * 1024 * 1024
  val blockSize64 = 64 * 1024 * 1024

  def read: SIO[DataFrame] = DataFrameUtil.readPaquet(pathInputParquet)

  val pipelineCalculateNbRows =
    Pipeline(
      read,
      DataFrameUtil.identityTransform,
      DataFrameUtil.calculateNbRows
    )

  val pipelineCalculateEstimatedRow =
    Pipeline(
      read,
      DataFrameUtil.identityTransform,
      DataFrameUtil.estimatedSizeDataFrame
    )

  def calculateNbRows: SIO[Unit] = for {
    _ <- ZIO.log("Calculating number of rows")
    nbRows <- pipelineCalculateNbRows.run
    _ <- ZIO.log(s"🔥🔥🔥🔥 nbRows: ${nbRows} 🔥🔥🔥🔥")
  } yield ()

  // Votre programme
  def program: SIO[Unit] = for {

    _ <- ZIO.log("\n🚀 🚀 🚀 Application started\n")

    estimatedSizeRow <- pipelineCalculateEstimatedRow.run

    nbRows <- pipelineCalculateNbRows.run

    estimatedSize <- ZIO.succeed(estimatedSizeRow.toInt * nbRows)

    numPartitions <- ZIO.succeed(estimatedSize / blockSize64).map(_.toInt)

    _ <- ZIO.log(s"\n🥳 Running pipeline with numPartitions: $numPartitions\n")
    _ <- Pipeline(
      read, // Read
      { df => DataFrameUtil.repartition(df, numPartitions) }, // Transformaton
      { df => DataFrameUtil.writeParquet(df, pathOutputParquet) } // Action
    ).run
    _ <- ZIO.log(s"\n🥳 End pipeline with numPartitions: $numPartitions\n")

    _ <- ZIO.log(s"✅ estimatedSizeRow: ${estimatedSizeRow}  🔥🔥🔥🔥")
    _ <- ZIO.log(s"✅ nbRows: ${nbRows} 🔥🔥🔥🔥")
    _ <- ZIO.log(s"✅ estimatedSize: ${estimatedSize} 🔥🔥🔥🔥")
    _ <- ZIO.log(s"✅ numPartitions: $numPartitions 🔥🔥🔥🔥")

    _ <- ZIO.log("\n🚀 🚀 🚀 Application finished\n")

  } yield ()

  private val session: ZLayer[Any, Throwable, SparkSession] =
    SparkSessionLayer.session

  override def run: ZIO[ZIOAppArgs, Any, Any] = program.provide(session)
}
