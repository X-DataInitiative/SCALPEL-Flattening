package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.functions._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.YearAndMonths
import fr.polytechnique.cmap.cnam.flattening.join.SSRRIPFlatTableJoiner


class SSRRIPFlatTableJoinerSuite extends SharedContext {

  "WriteAsParquet" should "flatten SSR and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "ssr")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table_PMSI_SSR"
    val configTest = conf.joinTableConfigs.head.copy(inputPath = Some(parquetTablesPath))
    val flattenedTableTest = new SSRRIPFlatTableJoiner(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDF = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/" +
      "SSR_Flat")
    val expectedDF_cols = expectedDF.columns.toList //.sorted.filter(column => !(column.contains("GHM")))
    val expectedDF_sorted = expectedDF.select(expectedDF_cols.map(col): _*)


    // When
    flattenedTableTest.join
    val result = sqlContext.read.parquet(resultPath)
    var result_cols = result.columns.toList.sorted
    result_cols = result_cols //.filter(column => !(column.contains("GHM")))
    val result_sorted = result.select(result_cols.map(col): _*)

    // Then
    assert(expectedDF_sorted.columns.toSet == result_sorted.columns.toSet)
    assert(expectedDF_sorted.count() == result_sorted.count())
  }

  "WriteAsParquetMonth" should "flatten SSR monthly and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "ssr_month")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table_PMSI_SSR"
    val ssrTest = conf.joinTableConfigs.head.copy(inputPath = Some(parquetTablesPath))
    val configTest = ssrTest.copy(onlyOutput = List(YearAndMonths(2019, List(7))))
    val flattenedTableTest = new SSRRIPFlatTableJoiner(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDF = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/" +
      "SSR_Flat_month")
    val expectedDF_cols = expectedDF.columns.toList //.sorted.filter(column => !(column.contains("GHM")))
    val expectedDF_sorted = expectedDF.select(expectedDF_cols.map(col): _*)


    // When
    flattenedTableTest.join
    val result = sqlContext.read.parquet(resultPath)
    var result_cols = result.columns.toList.sorted
    result_cols = result_cols //.filter(column => !(column.contains("GHM")))
    val result_sorted = result.select(result_cols.map(col): _*)

    // Then
    assert(expectedDF_sorted.columns.toSet == result_sorted.columns.toSet)
    assert(expectedDF_sorted.count() == result_sorted.count())
  }

}
