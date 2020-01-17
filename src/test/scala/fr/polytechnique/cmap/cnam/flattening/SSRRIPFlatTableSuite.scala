package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.SharedContext
import org.apache.spark.sql.functions._

class SSRRIPFlatTableSuite extends SharedContext {

  "WriteAsParquet" should "flatten SSR and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "ssr")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val configTest = conf.joinTableConfigs.head.copy(inputPath = Some(parquetTablesPath))
    val flattenedTableTest = new SSRRIPFlatTable(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDF = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/" +
      "SSR_Flat/SSR_flat.parquet")
    val expectedDF_cols = expectedDF.columns.toList//.sorted.filter(column => !(column.contains("GHM")))
    val expectedDF_sorted = expectedDF.select(expectedDF_cols.map(col):_*)


    // When
    flattenedTableTest.writeAsParquet()
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