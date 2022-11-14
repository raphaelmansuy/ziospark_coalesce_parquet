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

import zio.cli.HelpDoc.Span.text
import zio.cli._
import zio.stream.{ZPipeline, ZSink, ZStream}

import java.nio.file.Path

// A cli app that reads a parquet file, coalesce it and write it to a new parquet file
object App extends ZIOCliDefault {

  sealed trait Subcommand
  object Subcommand {
    final case class Coalesce(
        inputPath: String,
        outputhPath: String,
        blockSize: Int
    ) extends Subcommand
  }

  val blockSizeOption = Options.integer("block-size") ?? "The block size in MB"
  val sourcePathArgument = Args.text("source-path") ?? "The source path such as s3a://bucket/path"
  val outputPathArgument = Args.text("output-path") ?? "The output path such as /tmp/output.parquet"

  val coalesceCommand =
    Command(
      "coalesce",
      blockSizeOption,
      sourcePathArgument ++ outputPathArgument
    ).map { case (blockSize, (source, output)) =>
      Subcommand.Coalesce(source, output, blockSize.toInt * 1024 * 1024)
    }

  val cliApp = CliApp.make(
    name = "Coalesce parquet files",
    version = "0.0.1",
    summary = text("Coalesce parquet files"),
    footer = HelpDoc.p("©Copyright 2022"),
    command = coalesceCommand
  )((subcommand: Subcommand) => {
    subcommand match {
      case Subcommand.Coalesce(inputPath, outputPath, blockSize) =>
        {
          for {
            _ <- coalescePipeline(
              inputPath,
              outputPath,
              blockSize
            )
          } yield ()
        }.provideLayer(session)
    }
  })

  def coalescePipeline(
      pathInputParquet: String,
      pathOutputParquet: String,
      blockSize: Int
  ): SIO[Unit] = for {
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

}
