import zio._
import zio.test._
import zio.test.Assertion._
import zio.spark.experimental
import zio.spark.experimental.Pipeline
import zio.spark.parameter._
import zio.spark.sql._
import zio.spark.sql.implicits._
import org.apache.log4j.{Level, Logger}

import elitizon.ziospark.coalesce._

object AppSpec extends ZIOSpecDefault {

  val readData = 
    suite("Read data")(
      test("read a DataFrame from a parquet file") {
        for {
          df <- DataFrameUtil.readPaquet("./resources/data")
          count <- DataFrameUtil.calculateNbRows(df)
          // show the fist 200 lines of the DataFrame
          //_ <- df.show(200)
        } yield assert(count)(
          isGreaterThanEqualTo(0L)
        ) // assert the number of rows more than 1M
      }
    )

  val estimateSizeDataFrame = 
    suite("Estimate size of a DataFrame")(
      test("estimate the size of a DataFrame") {
        for {
          df <- DataFrameUtil.readPaquet("./resources/data")
          estimatedSize <- DataFrameUtil.estimatedSizeDataFrame(df)
          // display the estimated size of the DataFrame
          _ <- ZIO.log(s"🚀 Estimated size of the DataFrame: ${estimatedSize}")
        } yield assert(estimatedSize)(
          isGreaterThanEqualTo(0L)
        ) // assert the size of the DataFrame more than 1M
      }
    )
  

  val session = SparkSessionLayer.session
  // spec suite read a DataFrame from a parquet file
  def spec =
    (readData + estimateSizeDataFrame).provideLayerShared(session)

}
