package elitizon.ziospark.coalesce 

import org.apache.spark.sql.Row

import zio._
import zio.spark.experimental
import zio.spark.experimental.Pipeline
import zio.spark.parameter._
import zio.spark.sql._
import zio.spark.sql.implicits._
import org.apache.log4j.{Level, Logger}

// Import SaveMode
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SaveMode

object App extends ZIOAppDefault {

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

//  val pathInputParquet =  "/Users/raphaelmansuy/Downloads/dkt_tables/bronze/store_item_follow"
  val pathInputParquet = "/Users/raphaelmansuy/Downloads/input_parquet/"
  val pathOutputParquet = "/Users/raphaelmansuy/Downloads/output_parquet/"
  val pathOutputParquetRepartition =
    "/Users/raphaelmansuy/Downloads/output_parquet_repartition/"

  // block size 128MB
  val blockSize128 = 128 * 1024 * 1024
  val blockSize64 = 64 * 1024 * 1024

  def read: SIO[DataFrame] = { readPaquet(pathInputParquet) }
  def repartition(inputDF: DataFrame, numPartitions: Int) = {
    inputDF.repartition(numPartitions)
  }
  def action(inputDF: DataFrame) = {
    for {
      dfPersisted <- inputDF.persist(StorageLevel.MEMORY_AND_DISK)
      dfResult <- dfPersisted.write
        .options(
          Map(
            "compression" -> "snappy",
            "parquet.block.size" -> f"$blockSize64"
          )
        )
        .mode(SaveMode.Overwrite)
        .parquet(pathOutputParquet)
    } yield inputDF
  }

  def actionCalculateNbRows(inputDF: DataFrame) = {
    for {
      count <- inputDF.count
    } yield count
  }

  def actionCalulateEstimatedSize(inputDF: DataFrame) = {
    for {
      estimatedSizeSample <- inputDF.sample(0.01).rdd.map(_.size).reduce(_ + _)
      estimatedSize = (estimatedSizeSample * 100.0)
    } yield estimatedSize
  }

  def identityTransform(inputDF: DataFrame) = { inputDF }

  val pipelineCalculateNbRows =
    Pipeline(read, identityTransform, actionCalculateNbRows)
  val pipelineCalculateEstimatedSize =
    Pipeline(read, identityTransform, actionCalulateEstimatedSize)

  def calculateNbRows: SIO[Unit] = for {
    _ <- ZIO.log("Calculating number of rows")
    nbRows <- pipelineCalculateNbRows.run
    _ <- ZIO.log(s"🔥🔥🔥🔥 nbRows: ${nbRows} 🔥🔥🔥🔥")
  } yield ()

  // Votre programme
  def program: SIO[Unit] = for {
    _ <- ZIO.log("\n🚀 🚀 🚀 Application started\n")

    estimatedSize <- pipelineCalculateEstimatedSize.run
    estimatedSizeMB <- ZIO.succeed(estimatedSize / 1024 / 1024)
    estimatedSizeMBFormatted <- ZIO.succeed(f"$estimatedSizeMB%1.2f")
    _ <- ZIO.log(
      s"🔥🔥🔥🔥 estimatedSize: $estimatedSizeMBFormatted MB 🔥🔥🔥🔥\n"
    )

    numPartitions <- ZIO.succeed(estimatedSize / blockSize64).map(_.toInt)

    _ <- ZIO.log(s"\n🔥🔥🔥🔥 numPartitions: $numPartitions 🔥🔥🔥🔥")

    _ <- ZIO.log(s"\n🥳 Running pipeline with numPartitions: 1\n")
    _ <- Pipeline(read, { df => repartition(df, numPartitions) }, action).run
    _ <- ZIO.log("\n🚀 🚀 🚀 Application finished\n")

  } yield ()

  def readPaquet(path: String): SIO[DataFrame] =
    for {
      _ <- ZIO.log(s"\n🎬 Reading parquet files: $path")
      df <- SparkSession.read.parquet(path)
      dfPersisted <- df.persist(StorageLevel.MEMORY_AND_DISK_SER)
      _ <- ZIO.log("\n🏁 End reading parquet files")
    } yield dfPersisted

  private val session: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {
      val disableSparkLogging: UIO[Unit] =
        ZIO.succeed(Logger.getLogger("org").setLevel(Level.OFF))

      val builder: SparkSession.Builder =
        SparkSession.builder.master(localAllNodes).appName("zio-ecosystem")

      val build: ZIO[Scope, Throwable, SparkSession] =
        builder.getOrCreate.withFinalizer { ss =>
          ZIO.logInfo("Closing Spark Session ...") *>
            ss.close
              .tapError(_ => ZIO.logError("Failed to close the Spark Session."))
              .orDie
        }

      ZIO.logInfo("Opening Spark Session...") *> disableSparkLogging *> build
    }

  override def run: ZIO[ZIOAppArgs, Any, Any] = program.provide(session)
}
