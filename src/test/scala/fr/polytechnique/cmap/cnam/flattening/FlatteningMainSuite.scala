package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils._

class FlatteningMainSuite extends SharedContext {


  "saveCSVTablesAsParquet" should "read all dummy csv tables and save as parquet files" in {
    //Given
    val conf = FlatteningConfig.load("", "test")
    val resultPath = conf.singleTablePath

    val expectedTables: List[String] = conf.tables.map(_.name)
    val expectedDfs: Map[String, DataFrame] = expectedTables.map {
      name =>
        name -> sqlContext.read
          .option("mergeSchema", "true")
          .load("src/test/resources/flattening/parquet-table/single_table/" + name)
          .toDF
    }.toMap

    //When
    FlatteningMain.saveCSVTablesAsParquet(sqlContext, conf)
    val result = expectedTables.map {
      name =>
        name -> sqlContext.read
          .option("mergeSchema", "true")
          .load(resultPath + "/" + name)
          .toDF
    }.toMap


    //Then
    import fr.polytechnique.cmap.cnam.utilities.DFUtils._
    expectedDfs.foreach {
      case (name, df) =>
        assert(df.sameAs(result(name), true))
    }
  }

  "computeFlattenedTable" should "flatten the tables correctly" in {

    //Given
    val conf = FlatteningConfig.load("", "test")
    val expectedMCO: DataFrame = sqlContext.read.option("mergeSchema", "true").parquet("src/test/resources/flattening/parquet-table/flat_table/MCO/")
    val expectedDCIR: DataFrame = sqlContext.read.option("mergeSchema", "true").parquet("src/test/resources/flattening/parquet-table/flat_table/by_month/DCIR/")
    val configTest = conf.copy(join = conf.join.map(_.copy(inputPath = Some("src/test/resources/flattening/parquet-table/single_table"))))

    //When
    FlatteningMain.computeFlattenedFiles(sqlContext, configTest)
    val resultMCO = sqlContext.read.option("mergeSchema", "true").parquet(conf.flatTablePath + "/MCO*")
    val resultDCIR = sqlContext.read.option("mergeSchema", "true").parquet(conf.flatTablePath + "/DCIR*")

    //Then
    assert(expectedMCO sameAs resultMCO)
    assert(expectedDCIR sameAs resultDCIR)

  }

  "run" should "convert the dummy csv into parquet then join them in a flattened file" in {

    //Given
    val sqlCtx = sqlContext
    val path = "target/test/output/flat_table/MCO"
    val expected: DataFrame = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/MCO")

    //When
    FlatteningMain.run(sqlCtx, Map("env" -> "test"))
    val joinedDF = sqlContext.read.parquet(path).withColumn("ETA_NUM", col("ETA_NUM").cast(sql.types.StringType))

    //Then
    assert(joinedDF.sameAs(expected, true))
  }
}
