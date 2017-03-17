package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._

/**
  * Created by sathiya on 24/02/17.
  */
class FlatteningMainSuite extends SharedContext {

  "saveCSVTablesAsParquet" should "read all dummy csv tables and save as parquet files" in {

    //Given
    val resultPath = FlatteningConfig.outputBasePath

    val expectedTables: List[String] = FlatteningConfig.tablesConfigList.map(config => config.name)
    val expectedDfs: Map[String, DataFrame]= expectedTables.map{
      name =>
        name -> sqlContext.read
          .option("mergeSchema", "true")
          .load("src/test/resources/flattening/parquet-table/" + name)
          .toDF
    }.toMap

    //When
    FlatteningMain.saveCSVTablesAsParquet(sqlContext)
    val result = expectedTables.map{
      name =>
        name -> sqlContext.read
          .option("mergeSchema", "true")
          .load(resultPath + "/" + name)
          .toDF
    }.toMap


    //Then
    import fr.polytechnique.cmap.cnam.utilities.DFUtils._
    expectedDfs.foreach{
      case(name, df) =>
        assert(df.sameAs(result(name)))
    }
  }
}
