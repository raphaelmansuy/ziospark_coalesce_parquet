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

  val pipelineCalculateEstimatedSize =
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

    estimatedSize <- pipelineCalculateEstimatedSize.run
    estimatedSizeMB <- ZIO.succeed(estimatedSize / 1024 / 1024)
    estimatedSizeMBFormatted <- ZIO.succeed(f"$estimatedSizeMB%1.2f")
    _ <- ZIO.log(
      s"🔥🔥🔥🔥 estimatedSize: $estimatedSizeMBFormatted MB 🔥🔥🔥🔥\n"
    )

    numPartitions <- ZIO.succeed(estimatedSize / blockSize64).map(_.toInt)

    _ <- ZIO.log(s"\n🔥🔥🔥🔥 numPartitions: $numPartitions 🔥🔥🔥🔥")

    _ <- ZIO.log(s"\n🥳 Running pipeline with numPartitions: $numPartitions\n")
    _ <- Pipeline(
      read, // Read
      { df => DataFrameUtil.repartition(df, numPartitions) }, // Transformaton
      { df => DataFrameUtil.writeParquet(df, pathOutputParquet) } // Action
    ).run
    _ <- ZIO.log(s"\n🥳 End pipeline with numPartitions: $numPartitions\n")

    _ <- ZIO.log("\n🚀 🚀 🚀 Application finished\n")

  } yield ()

  private val session: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {
      val disableSparkLogging: UIO[Unit] =
        ZIO.succeed(Logger.getLogger("org").setLevel(Level.OFF))

      val appName = "ziospark-parquet-coalesce"

      val builder: SparkSession.Builder =
        SparkSession.builder.master(localAllNodes).appName(appName)

      // configure S3 access
      val builderWithS3 = builder
        .config("spark.hadoop.fs.s3a.access.key", "AKIAJ2Z7Z7Z7Z7Z7Z7Z7")
        .config("spark.hadoop.fs.s3a.secret.key", "secret")
        .config(
          "spark.hadoop.fs.s3a.impl",
          "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
          "fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider"
        )
        .config("spark.hadoop.fs.s3a.connection.maximum", "1000")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "100")

      val finalBuilder = builderWithS3

      val build: ZIO[Scope, Throwable, SparkSession] =
        finalBuilder.getOrCreate.withFinalizer { ss =>
          ZIO.logInfo("Closing Spark Session ...") *>
            ss.close
              .tapError(_ => ZIO.logError("Failed to close the Spark Session."))
              .orDie
        }

      ZIO.logInfo("Opening Spark Session...") *> disableSparkLogging *> build
    }

  override def run: ZIO[ZIOAppArgs, Any, Any] = program.provide(session)
}
