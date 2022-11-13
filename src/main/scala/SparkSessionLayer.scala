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

object SparkSessionLayer {

  import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

  val session: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {
      val disableSparkLogging: UIO[Unit] =
        ZIO.succeed(Logger.getLogger("org").setLevel(Level.OFF))

      val appName = "ziospark-parquet-coalesce"

      val builder: SparkSession.Builder =
        SparkSession.builder
          .master(localAllNodes)
          .appName(appName)
          // store data on disk to avoid memory overflow
          .config("spark.sql.autoBroadcastJoinThreshold", -1)
          

          

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
}
