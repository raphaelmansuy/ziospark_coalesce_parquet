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

object DataFrameUtil {

  // block size 128MB
  val blockSize128 = 128 * 1024 * 1024
  // block size 64MB
  val blockSize64 = 64 * 1024 * 1024

  // identity transform
  def identityTransform(inputDF: DataFrame) = { inputDF }

  // Read parquet file from a path in memory and disk (StorageLevel.MEMORY_AND_DISK)
  // and return a DataFrame
  def readPaquet(path: String): SIO[DataFrame] =
    for {
      _ <- ZIO.log(s"\n🎬 Reading parquet files: $path")
      df <- SparkSession.read.parquet(path)
      dfPersisted <- df.persist(StorageLevel.MEMORY_AND_DISK_SER)
      _ <- ZIO.log("\n🏁 End reading parquet files")
    } yield dfPersisted

  // Write a dataframe to a a path uing the snappy compression
  // and a block size of 64MB
  def writeParquet(inputDF: DataFrame, pathOutputParquet: String) = {
    for {
      dfResult <- inputDF.write
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

  def repartition(inputDF: DataFrame, numPartitions: Int) = {
    inputDF.repartition(numPartitions)
  }

	def calculateNbRows(inputDF: DataFrame) = {
    for {
      count <- inputDF.count
    } yield count
  }

  def estimatedSizeDataFrame(inputDF: DataFrame) = {
    for {
      estimatedSizeSample <- inputDF.sample(0.01).rdd.map(_.size).reduce(_ + _)
      estimatedSize = (estimatedSizeSample * 100.0)
    } yield estimatedSize
  }

}
