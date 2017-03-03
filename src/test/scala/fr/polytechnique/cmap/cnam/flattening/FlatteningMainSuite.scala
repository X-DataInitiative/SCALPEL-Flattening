package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 24/02/17.
  */
class FlatteningMainSuite extends SharedContext {

  "saveCSVTablesAsParquet" should "read all dummy csv tables and save as parquet files" in {

    //Given
    val expectedResultPathRoot: String = "src/test/resources/flattening/parquet-table"
    val expectedResultPath: List[String] = FlatteningConfig.tablesConfigOutputPath
      .map(expectedResultPathRoot + "/" + _)
    val resultPathRoot = FlatteningConfig.outputPath
    val resultPath = FlatteningConfig.tablesConfigOutputPath.map(resultPathRoot + "/" + _)

    import fr.polytechnique.cmap.cnam.utilities.FileSystemUtils._
    val expectedResult: Seq[DataFrame]= readAllParquetDFsFrom(expectedResultPath, sqlContext)

    //When
    FlatteningMain.saveCSVTablesAsParquet(sqlContext)
    val result = readAllParquetDFsFrom(resultPath, sqlContext)

    //Then
    println("result DFs count: " + result.size)
    println("expectedResult DFs count: " + expectedResult.size)

    import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
    for(i <- 0 to Math.max(result.size - 1,  expectedResult.size - 1)) {
      if(result.apply(i) ==== expectedResult.apply(i)) {
        println(s"${resultPath.apply(i)} equals ${expectedResultPath.apply(i)}")
        assert(true)
      }
      else {
        println(s"${resultPath.apply(i)} NOT equal to ${expectedResultPath.apply(i)}")
        assert(false)
      }
    }
  }
}
