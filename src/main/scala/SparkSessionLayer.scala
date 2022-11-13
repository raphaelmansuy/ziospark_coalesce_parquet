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
         // .config("spark.driver.memory", "4g") // 4g
         // .config("spark.executor.memory", "2g") // 2g
         // .config("spark.driver.maxResultSize", "2g") // 2g
         // .config("spark.sql.files.maxPartitionBytes", "128m") // 128MB
         // .config("spark.sql.files.openCostInBytes", "128m") // default 4MB
         // .config("spark.sql.files.ignoreCorruptFiles", "true") // https://stackoverflow.com/questions/50888113/spark-sql-exception-when-reading-parquet-file
         // .config("spark.sql.files.ignoreMissingFiles", "true") // https://stackoverflow.com/questions/50888113/spark-2-3-1-ignoring-missing-files
         // .config("spark.sql.files.maxRecordsPerFile", "1000000") // 1M
          // optimize shuffle
         // .config("spark.sql.shuffle.partitions", "100") // default 200
         // .config("spark.sql.autoBroadcastJoinThreshold", "104857600") // 100MB
         // .config("spark.sql.adaptive.enabled", "true") // enable adaptive query execution
         // .config("spark.sql.adaptive.coalescePartitions.enabled", "true") // coalesce partitions
          // optimize memory for a small cluster (1 driver + 2 executors) with 4GB each
         // .config("spark.memory.fraction", "0.6") // default 0.75
         // .config("spark.memory.storageFraction", "0.3") // default 0.5
         // .config("spark.memory.offHeap.enabled", "true") // default false
         // .config("spark.memory.offHeap.size", "1g") // 1GB
         // .config("spark.executor.memoryOverhead", "1g") // 1GB
         // .config("spark.driver.memoryOverhead", "1g") // 1GB
         // .config("spark.sql.broadcastTimeout", "36000") // 10h
         // .config("spark.sql.autoBroadcastJoinThreshold", "104857600") // 100MB
         // .config("spark.sql.files.maxPartitionBytes", "128m") // 128MB

          

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
