package fr.polytechnique.cmap.cnam.statistics

import java.sql.Date
import org.apache.spark.sql.types.TimestampType
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class FlatTableHelperSuite extends SharedContext {

  "changeColumnNameDelimiter" should "change the column name delimiters from dot to underscore " in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDf = Seq(
      (1, 1.0, "val1", "val2")
    ).toDF("key1", "noDelimiter", "key.delimiter1", "key.delimiter2")

    val expectedResult = Seq(
      (1, 1.0, "val1", "val2")
    ).toDF("key1", "noDelimiter", "key__delimiter1", "key__delimiter2")

    // When
    import FlatTableHelper.ImplicitDF
    val result = inputDf.changeColumnNameDelimiter

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expectedResult)
  }

  "changeSchema" should "change the column types to the passed format" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDf = Seq(
      ("1", "1", "1", "2006-2-20")
    ).toDF("BEN_CTA_TYP", "CPL_REM_BSE", "ER_PHA_F__PHA_PRS_C13", "FLX_TRT_DTD")

    val inputSchema: List[TableSchema] = List(
      TableSchema("ER_PRS_F", Map("BEN_CTA_TYP" -> "Integer")),
      TableSchema("ER_PRS_F", Map("CPL_REM_BSE" -> "Double")),
      TableSchema("ER_PRS_F", Map("FLX_TRT_DTD" -> "Date")),
      TableSchema("ER_PHA_F", Map("PHA_PRS_C13" -> "Long"))
    )
    val expectedResult = Seq(
      (1, 1.0, 1L, Date.valueOf("2006-02-20"))
    ).toDF(inputDf.columns: _*)

    val mainTableName = "ER_PRS_F"
    val dateFormat = "yyyy-MM-dd"

    // When
    import FlatTableHelper.ImplicitDF
    val result = inputDf.changeSchema(inputSchema, mainTableName, dateFormat)

    // Then
    expectedResult.show
    expectedResult.printSchema
    result.show
    result.printSchema

    import utilities.DFUtils.CSVDataFrame
    assert(inputDf.schema != result.schema)
    assert(result sameAs expectedResult)
  }

  it should "consider default date format as dd/MM/yyyy when it is not specified explicitly" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputColumn = "FLX_TRT_DTD"
    val inputDf = Seq("20/02/2006").toDF(inputColumn)

    import org.apache.spark.sql.functions._
    val dateFormat = "dd/MM/yyyy"
    val expectedResultColumn = to_date(
      unix_timestamp(inputDf(inputColumn), dateFormat).cast(TimestampType)
    ).as(inputColumn)

    val expectedResult = inputDf.select(expectedResultColumn)
    val schemaMap = List(TableSchema("ER_PRS_F", Map("FLX_TRT_DTD" -> "Date")))

    val mainTableName = "ER_PRS_F"

    // When
    import FlatTableHelper.ImplicitDF
    val result = inputDf.changeSchema(schemaMap, mainTableName)

    // Then
    inputDf.printSchema
    inputDf.show
    expectedResult.show
    result.show
    result.printSchema

    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expectedResult)
    assert(inputDf.schema != result.schema)
  }

  "annotateJoiningTablesColumns" should "prefix table name to the column names of the joining " +
      "tables" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val testInput: List[TableSchema] = List(
      TableSchema("ER_PRS_F", Map("Col1PRS" -> "Type1")),
      TableSchema("ER_PRS_F", Map("Col2PRS" -> "Type2")),
      TableSchema("ER_PHA_F", Map("Col1" -> "Type1")),
      TableSchema("ER_PHA_F", Map("Col2" -> "Type2")),
      TableSchema("Random", Map("Col1" -> "Type1")),
      TableSchema("Random", Map("Col2" -> "Type2"))
    )
    val expectedResult: List[Map[String, String]] = List(
      Map("Col1PRS" -> "Type1"),
      Map("Col2PRS" -> "Type2"),
      Map("ER_PHA_F__Col1" -> "Type1"),
      Map("ER_PHA_F__Col2" -> "Type2"),
      Map("Random__Col1" -> "Type1"),
      Map("Random__Col2" -> "Type2")
    )
    val mainTableName = "ER_PRS_F"
    val sampleDf = Seq("dummyValue").toDF("Col_1")

    // When
    import FlatTableHelper.ImplicitDF
    val testResult = testInput.map { testInput =>
      sampleDf.annotateJoiningTablesColumns(testInput, mainTableName)
    }

    // Then
    testResult foreach println
    assert(testResult === expectedResult)
  }

  "prefixColumnName" should "concatenate two given string with __ (double underscores)" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val tableName = "ER_PHA_F"
    val columnName = "FLX_TRT_DTD"
    val expectedResult = tableName + "__" + columnName
    val sampleDf = Seq("dummyValue").toDF("Col_1")

    // When
    import FlatTableHelper.ImplicitDF
    val result = sampleDf.prefixColName(tableName, columnName)

    // Then
    assert(result == expectedResult)
  }

  "writeStatistics" should "compute statistics on the input DF and write it in a given path" in {

    // Given
    val inputDfPath = "src/test/resources/statistics/flat_table/input/newMCO"
    val expectedResultPath = "src/test/resources/statistics/flat_table/expected/newMCOStat"
    val resultPath = "target/test/output/statistics/newMCO"
    val inputDf = sqlContext.read.option("mergeSchema", "true").parquet(inputDfPath).drop("year")
    val expectedResult = sqlContext.read.parquet(expectedResultPath)

    // When
    import FlatTableHelper.ImplicitDF
    inputDf.writeStatistics(resultPath)
    val result = sqlContext.read.parquet(resultPath)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expectedResult)

  }

}
