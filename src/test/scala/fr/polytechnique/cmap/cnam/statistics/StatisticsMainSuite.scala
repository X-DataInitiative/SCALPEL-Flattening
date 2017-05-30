package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.Row
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class StatisticsMainSuite extends SharedContext{

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
      ("1", "2", 2L, None, "SINGLE__NUM_ENQ"),
      ("10", "20", 2L, Some(30D), "SINGLE__NUMERIC_COL"),
      ("1", "10", 2L, None, "SINGLE__STRING_COL")
    ).toDF("Min", "Max", "CountDistinct", "SumDistinct", "ColName")

    // When
    val result = StatisticsMain.computeSingleTableStats(input, isCentral, singleConf)

    // Then
    import utilities.DFUtils.CSVDataFrame
    result.show
    expected.show
    assert(result sameAs expected)
  }

  "describeFlatTable" should "write the statistics for a given flat table" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val flatConf = FlatTableConfig(
      tableName = "FLAT",
      centralTable = "CENTRAL",
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
      ("1", "2", 3L, 2L, None, None, None, "NUM_ENQ"),
      ("10", "20", 3L, 2L, Some(50D), Some(30D), Some(16.6667D), "NUMERIC_COL"),
      ("1", "10", 3L, 2L, None, None, None,  "OTHER__STRING_COL")
    ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName")
    val expectedSingle = Seq(
      ("1", "2", 2L, None, "NUM_ENQ"),
      ("10", "20", 2L, Some(30D), "NUMERIC_COL"),
      ("1", "10", 2L, None, "OTHER__STRING_COL"),
      ("1", "2", 2L, None, "OTHER__NUM_ENQ"),
      ("10", "20", 2L, Some(30D), "OTHER__NUMERIC_COL")
    ).toDF("Min", "Max", "CountDistinct", "SumDistinct", "ColName")
    val expectedDiff = sqlContext.createDataFrame(sc.parallelize(List[Row]()), expectedSingle.schema)


    // When
    StatisticsMain.describeFlatTable(input, flatConf)
    val resultFlat = spark.read.parquet("target/test/output/stats/flat_table")
    val resultSingle = spark.read.parquet("target/test/output/stats/single_tables")
    val diff = spark.read.parquet("target/test/output/stats/diff")

    // Then
    import utilities.DFUtils.CSVDataFrame
    resultSingle.show
    expectedSingle.show
    assert(resultFlat sameAs expectedFlat)
    assert(resultSingle sameAs expectedSingle)
    assert(diff sameAs expectedDiff)
  }

  "run" should "run the overall pipeline correctly without any error" in {

    // Given
    val expectedResultPath = "src/test/resources/statistics/flat_table/expected/newMCOStat"
    val expected = sqlContext.read.parquet(expectedResultPath)

    // When
    StatisticsMain.run(sqlContext, Map("conf" -> "src/main/resources/statistics/test.conf"))
    val resultOld = spark.read.parquet("target/test/output/stats/oldMCO/flat_table")
    val resultNew = spark.read.parquet("target/test/output/stats/newMCO/flat_table")

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(resultOld sameAs expected)
    assert(resultNew sameAs expected)
  }
}
