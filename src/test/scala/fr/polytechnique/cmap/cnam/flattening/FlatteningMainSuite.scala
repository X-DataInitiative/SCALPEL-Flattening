package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql._
import com.typesafe.config.ConfigValueFactory
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import org.apache.spark.sql
import org.apache.spark.sql.functions.col

class FlatteningMainSuite extends SharedContext {


  "saveCSVTablesAsParquet" should "read all dummy csv tables and save as parquet files" in {
    import FlatteningConfig.SingleTableConfig
    //Given
    val resultPath = FlatteningConfig.outputBasePath

    val expectedTables: List[String] = FlatteningConfig.tablesConfigList.map(config => config.name)
    val expectedDfs: Map[String, DataFrame]= expectedTables.map{
      name =>
        name -> sqlContext.read
          .option("mergeSchema", "true")
          .load("src/test/resources/flattening/parquet-table/single_table/" + name)
          .toDF
    }.toMap

    //When
    FlatteningMain.saveCSVTablesAsParquet(sqlContext)
    val result = expectedTables.map{
      name =>
        name -> sqlContext.read
          .option("mergeSchema", "true")
          .load(resultPath + "/" + name )
          .toDF
    }.toMap


    //Then
    import fr.polytechnique.cmap.cnam.utilities.DFUtils._
    expectedDfs.foreach{
      case(name, df) =>
        assert(df.sameAs(result(name), true))
    }
  }

  "computeFlattenedTable" should "flatten the dcir tables correctly" in {

    //Given
    val sqlCtx = sqlContext
    val expected: DataFrame = sqlCtx.read.option("mergeSchema","true").parquet("src/test/resources/flattening/parquet-table/flat_table/MCO/")
    val configTest = FlatteningConfig.joinTablesConfig.map(
      x =>
        x.withValue("input_path", ConfigValueFactory.fromAnyRef("src/test/resources/flattening/parquet-table/single_table"))
    )

    //When
    FlatteningMain.computeFlattenedFiles(sqlCtx, configTest)
    val result = sqlCtx.read.option("mergeSchema","true").parquet("target/test/output/join/MCO*")

    //Then
    assert(expected sameAs result)

  }

  "run" should "convert the dummy csv into parquet then join them in a flattened file" in {

    //Given
    val sqlCtx = sqlContext
    val path = "target/test/output/join/MCO"
    val expected: DataFrame = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/MCO")

    //When
    FlatteningMain.run(sqlCtx, Map())
    val joinedDF = sqlContext.read.parquet(path).withColumn("ETA_NUM", col("ETA_NUM").cast(sql.types.StringType))

    //Then
    assert(joinedDF.sameAs(expected, true))
  }
}
