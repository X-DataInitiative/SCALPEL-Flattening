package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.YearAndMonths

class PMSIFlatTableSuite extends SharedContext {

  "writeAsParquet" should "flatten MCO and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "test_PMSI")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table_PMSI_SSR"
    val configTest = conf.joinTableConfigs.head.copy(inputPath = Some(parquetTablesPath))
    val flattenedTableTest = new PMSIFlatTable(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDF = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/" +
      "PMSI_Flat/PMSI_flat.parquet")
    val expectedDF_cols = expectedDF.columns.toList//.sorted.filter(column => !(column.contains("GHM")))
    val expectedDF_sorted = expectedDF.select(expectedDF_cols.map(col):_*)


    // When
    flattenedTableTest.writeAsParquetAndORC()
    val result = sqlContext.read.parquet(resultPath)
    var result_cols = result.columns.toList.sorted
    result_cols = result_cols//.filter(column => !(column.contains("GHM")))
    val result_sorted = result.select(result_cols.map(col):_*)

    // Then
    assert(resultPath == flattenedTableTest.outputBasePath)
    assert(expectedDF_sorted.columns.toSet == result_sorted.columns.toSet)
    assert(expectedDF_sorted.count() == result_sorted.count())
  }

  "writeAsParquetMonth" should "flatten MCO monthly and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "test_PMSI_month")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table_PMSI_SSR"
    val pmsiTest = conf.joinTableConfigs.head.copy(inputPath = Some(parquetTablesPath))
    val configTest = pmsiTest.copy(onlyOutput = List(YearAndMonths(2006, List(7)),YearAndMonths(2007, List(7)),YearAndMonths(2008, List(7))))
    val flattenedTableTest = new PMSIFlatTable(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDF = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/" +
      "PMSI_Flat_month")
    val expectedDF_cols = expectedDF.columns.toList//.sorted.filter(column => !(column.contains("GHM")))
    val expectedDF_sorted = expectedDF.select(expectedDF_cols.map(col):_*)


    // When
    flattenedTableTest.writeAsParquetAndORC()
    val result = sqlContext.read.parquet(resultPath)
    var result_cols = result.columns.toList.sorted
    result_cols = result_cols//.filter(column => !(column.contains("GHM")))
    val result_sorted = result.select(result_cols.map(col):_*)

    // Then
    assert(resultPath == flattenedTableTest.outputBasePath)
    assert(expectedDF_sorted.columns.toSet == result_sorted.columns.toSet)
    assert(expectedDF_sorted.count() == result_sorted.count())
  }

}
