package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp
import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext

class UDFsSuite extends SharedContext {

  "parseTimestamp" should "convert a String column to a Timestamp column" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val pattern = "dd/MM/yyyy HH:mm"
    val input = Seq(
      Some("15/01/2010 23:50"),
      Some(""),
      Some(" "),
      None
    ).toDS.toDF("string_col")
    val expected = Seq(
      Some(Timestamp.valueOf("2010-01-15 23:50:00")), None, None, None
    ).toDS.toDF("timestamp_col")

    // When
    val result = input.select(UDFs.parseTimestamp(pattern)(col("string_col")).as("timestamp_col"))

    // Then
    import DFUtils.CSVDataFrame
    assert(result sameAs expected)
  }

  it should "throw an exception if a string is unparseable" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val pattern = "dd/MM/yyyy HH:mm"
    val input = Seq(
      Some("2010/01/15 23:50")
    ).toDS.toDF("string_col")

    // WhenThen
    intercept[org.apache.spark.SparkException] {
      input.select(UDFs.parseTimestamp(pattern)(col("string_col"))).show
    }
  }

  "parseDate" should "convert a String column to a Date column" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val pattern = "dd/MM/yyyy"
    val input = Seq(
      Some("15/01/2010"),
      Some(""),
      Some(" "),
      None
    ).toDS.toDF("string_col")
    val expected = Seq(
      Some(java.sql.Date.valueOf("2010-01-15")), None, None, None
    ).toDS.toDF("date_col")

    // When
    val result = input.select(UDFs.parseDate(pattern)(col("string_col")).as("date_col"))

    // Then
    import DFUtils.CSVDataFrame
    assert(result sameAs expected)
  }

  it should "throw an exception if a string is unparseable" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val pattern = "dd/MM/yyyy"
    val input = Seq(
      Some("2010/01/15")
    ).toDS.toDF("string_col")

    // WhenThen
    intercept[org.apache.spark.SparkException] {
      input.select(UDFs.parseDate(pattern)(col("string_col"))).show
    }
  }
}
