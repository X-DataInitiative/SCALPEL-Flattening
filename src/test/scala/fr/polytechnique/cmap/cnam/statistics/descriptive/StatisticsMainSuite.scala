package fr.polytechnique.cmap.cnam.statistics.descriptive

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

    // Given
    val left = spark.read
      .option("header", true)
      .option("delimiter", ";")
      .csv("./src/test/resources/flattening/csv-table/left.csv")

    val right = spark.read
      .option("header", true)
      .option("delimiter", ";")
      .csv("./src/test/resources/flattening/csv-table/right.csv")
    left.write.parquet("target/test/output/left")
    right.write.parquet("target/test/output/right")

    val flat = left.join(right, List("id"), "left")
    flat.write.parquet("target/test/output/flat")


    val flatConf = FlatTableConfig(
      name = "FLAT",
      centralTable = "CENTRAL",
      joinKeys = List("id"),
      dateFormat = "yyyy-MM-dd",
      inputPath = "target/test/output/flat",
      outputStatPath = "target/test/output/stats",
      singleTables = List(
        SingleTableConfig("CENTRAL", "target/test/output/left"),
        SingleTableConfig("OTHER", "target/test/output/right")
      )
    )

    // When
    StatisticsMain.describeFlatTable(flat, flatConf)
    val diff = spark.read.parquet("target/test/output/stats/diff")
    val keys = spark.read.parquet("target/test/output/stats/diff_join_keys")

    // Then
    assert(diff.count() > 0)
    assert(keys.count() == 1)
  }

  "exceptOnColumns" should "compute the diff between flat_table stats and single_tables stats" in {

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

  "exceptOnJoinKeys" should "compute the diff join keys between flat_table stats and single_tables" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colNames = List("key1", "key2", "key3")

    val flat = Seq(
      ("1", "2", 2L),
      ("10", "20", 4L),
      ("100", "200", 6L)
    ).toDF(colNames: _*)

    val single = Seq(
      ("1", "2", 2L),
      ("10", "20", 4L),
      ("100", "200", 6L),
      ("1000", "2000", 8L)
    ).toDF(colNames: _*)

    val expected = Seq(
      ("1000", "2000", 8L, "flat")
    ).toDF("key1", "key2", "key3", "TableName")

    //When
    val result = StatisticsMain.exceptOnJoinKeys(flat, Map("flat" -> single))

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
    StatisticsMain.run(sqlContext, Map("conf" -> "src/main/resources/statistics/test.conf", "env" -> "test"))

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
