package fr.polytechnique.cmap.cnam.statistics.descriptive

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class StatisticsMainSuite extends SharedContext {

  "computeSingleTableStats" should "return the statistics for a given single table" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val isCentral = false
    val singleConf = SingleTableConfig("SINGLE", "/path/to/something/")
    val input = Seq(
      ("1", 10, "1"), ("2", 20, "10"), ("2", 20, "10")
    ).toDF("NUM_ENQ", "NUMERIC_COL", "STRING_COL")
    val expected = Seq(
      ("1", "2", 2L, None, "SINGLE__NUM_ENQ", "StringType", "SINGLE"),
      ("10", "20", 2L, Some(30D), "SINGLE__NUMERIC_COL", "IntegerType", "SINGLE"),
      ("1", "10", 2L, None, "SINGLE__STRING_COL", "StringType", "SINGLE")
    ).toDF("Min", "Max", "CountDistinct", "SumDistinct", "ColName", "ColType", "TableName")

    // When
    val result = StatisticsMain.computeSingleTableStats(input, isCentral, singleConf)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expected)
  }

  "describeFlatTable" should "write the statistics for a given flat table" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val flatConf = FlatTableConfig(
      tableName = "FLAT",
      centralTable = "CENTRAL",
      joinKeys = List("NUM_ENQ", "NUMERIC_COL"),
      dateFormat = "yyyy-MM-dd",
      inputPath = "/path/to/something/",
      outputStatPath = "target/test/output/stats",
      singleTables = List(
        SingleTableConfig("CENTRAL", "src/test/resources/statistics/single-tables/CENTRAL"),
        SingleTableConfig("OTHER", "src/test/resources/statistics/single-tables/OTHER")
      )
    )
    val input = Seq(
      ("1", 10, "1"),
      ("2", 20, "10"),
      ("2", 20, "10")
    ).toDF("NUM_ENQ", "NUMERIC_COL", "OTHER__STRING_COL")
    val expectedFlat = Seq(
      ("1", "2", 3L, 2L, None, None, None, "NUM_ENQ", "StringType", "FLAT"),
      ("10", "20", 3L, 2L, Some(50D), Some(30D), Some(16.6667D), "NUMERIC_COL", "IntegerType", "FLAT"),
      ("1", "10", 3L, 2L, None, None, None,  "OTHER__STRING_COL", "StringType", "FLAT")
    ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName", "ColType", "TableName")
    val expectedSingle = Seq(
      ("1", "2", 2L, None, "NUM_ENQ", "StringType", "CENTRAL"),
      ("10", "20", 2L, Some(30D), "NUMERIC_COL", "IntegerType", "CENTRAL"),
      ("1", "10", 2L, None, "OTHER__STRING_COL", "StringType", "OTHER")
    ).toDF("Min", "Max", "CountDistinct", "SumDistinct", "ColName", "ColType", "TableName")
    val expectedDiff = sqlContext.createDataFrame(sc.parallelize(List[Row]()), expectedSingle.schema)


    // When
    StatisticsMain.describeFlatTable(input, flatConf)
    val resultFlat = spark.read.parquet("target/test/output/stats/flat_table")
    val resultSingle = spark.read.parquet("target/test/output/stats/single_tables")
    val diff = spark.read.parquet("target/test/output/stats/diff")

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(resultFlat sameAs expectedFlat)
    assert(resultSingle sameAs expectedSingle)
    assert(diff sameAs expectedDiff)
  }

  "diff" should "compute the diff between flat_table stats and single_tables stats" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List("Min", "Max", "CountDistinct", "SumDistinct", "ColName", "ColType", "TableName")
    val flat = Seq(
      ("1", "2", 2L, None, "NUM_ENQ", "StringType", "FLAT"),
      ("10", "20", 2L, Some(40D), "NUMERIC_COL", "IntegerType", "FLAT"),
      ("1", "10", 2L, None, "OTHER__STRING_COL", "StringType", "FLAT"),
      ("1", "10", 2L, None, "CUSTOM_FLAT_COL", "StringType", "FLAT")
    ).toDF(colNames: _*)
    val single = Seq(
      ("1", "2", 2L, None, "NUM_ENQ", "StringType", "CENTRAL"),
      ("10", "20", 2L, Some(30D), "NUMERIC_COL", "IntegerType", "CENTRAL"),
      ("1", "10", 2L, None, "OTHER__STRING_COL", "StringType", "OTHER"),
      ("1", "10", 2L, None, "OTHER__CUSTOM_COL", "StringType", "OTHER")
    ).toDF(colNames: _*)
    val expected = Seq(
      ("10", "20", 2L, Some(40D), "NUMERIC_COL", "IntegerType", "FLAT"),
      ("10", "20", 2L, Some(30D), "NUMERIC_COL", "IntegerType", "CENTRAL"),
      ("1", "10", 2L, None, "OTHER__CUSTOM_COL", "StringType", "OTHER"),
      ("1", "10", 2L, None, "CUSTOM_FLAT_COL", "StringType", "FLAT")
    ).toDF(colNames: _*)

    // When
    val result = StatisticsMain.exceptOnColumns(flat, single, (colNames.toSet - "TableName").toList)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expected)
  }

  "run" should "run the overall pipeline correctly without any error" in {

    // Given
    val mcoExpectedResultPath = "src/test/resources/statistics/flat_table/expected/newMCOStat"
    val dcirExpectedResultPath = "src/test/resources/statistics/flat_table/expected/newDCIRStat"
    val mcoExpectedResult = sqlContext.read.parquet(mcoExpectedResultPath)
    val dcirExpectedResult = sqlContext.read.parquet(dcirExpectedResultPath)
    val outputRootPath = "target/test/output/stats"

    // When
    StatisticsMain.run(sqlContext, Map("conf" -> "src/main/resources/statistics/test.conf"))

    val oldDcirFlatTableStat = spark.read.parquet(outputRootPath + "/oldDCIR/flat_table")
    val oldMcoFlatTableStat = spark.read.parquet(outputRootPath + "/oldMCO/flat_table")

    // Then

    // We validate only oldFlat stat with expected output. For other outputs we only check
    // whether the needed output are generated under right output path and are readable as parquet
    spark.read.parquet(outputRootPath + "/newDCIR/flat_table")
    spark.read.parquet(outputRootPath + "/newDCIR/single_tables")
    spark.read.parquet(outputRootPath + "/newDCIR/diff")

    spark.read.parquet(outputRootPath + "/newMCO/flat_table")
    spark.read.parquet(outputRootPath + "/newMCO/single_tables")
    spark.read.parquet(outputRootPath + "/newMCO/diff")

    import utilities.DFUtils.CSVDataFrame
    assert(oldMcoFlatTableStat sameAs mcoExpectedResult)
    assert(oldDcirFlatTableStat sameAs dcirExpectedResult)

  }
}
