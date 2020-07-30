// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.convert.CSVConverter
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import fr.polytechnique.cmap.cnam.utilities.Functions

class FlatteningSuite extends SharedContext {

  it should "read all dummy csv tables and merge schema for orc" in {
    val sqlCtx = sqlContext
    //Given
    val nullable = true

    val conf = ConfigPartition(name = "dummy", inputPaths = List("src/test/resources/flattening/csv-table/dummy.csv"), dateFormat = "dd/MM/yyyy", output = "target/test/output")

    val inputSchema = List(
      ("BEN_NAI_ANN", "Integer"),
      ("BEN_DTE_MAJ", "Date"),
      ("NUM_ENQ", "String")
    )
    val schema = Map("dummy" -> inputSchema)

    val expectedResult = StructType(Seq(
      StructField("NUM_ENQ", StringType, nullable),
      StructField("BEN_DTE_MAJ", DateType, nullable),
      StructField("BEN_NAI_ANN", IntegerType, nullable)
    ))

    val expectedRDD = sc.parallelize(
      Seq(
        Row("john", Functions.parseDate("01/01/2006", "dd/MM/yyyy").get, null),
        Row("george", Functions.parseDate("25/01/2006", "dd/MM/yyyy").get, null),
        Row("john", null, null)
      )
    )
    val expectedDF = spark.createDataFrame(expectedRDD, expectedResult)
    //When
    val table = CSVConverter.read(sqlCtx, conf, schema, "orc")
    //Then
    assert(table.df.schema == expectedResult)
    assertDFs(table.df, expectedDF)

  }

  "saveCSVTablesAsSingleTables" should "read all dummy csv tables and save as single tables" in {
    //Given
    val conf = FlatteningConfig.load("", "test")

    val expectedTables: List[String] = conf.tables.map(_.name)
    val expectedDfs: Map[String, DataFrame] = expectedTables.map {
      name =>
        name -> sqlContext.read
          .option("mergeSchema", "true")
          .load("src/test/resources/flattening/parquet-table/single_table/" + name)
          .toDF
    }.toMap

    //When
    val meta = Flattening.saveCSVTablesAsSingleTables(sqlContext, conf)
    val result = meta.map {
      op =>
        op.outputTable -> sqlContext.read
          .option("mergeSchema", "true")
          .load(op.outputPath + "/" + op.outputTable)
          .toDF
    }.toMap


    //Then
    import fr.polytechnique.cmap.cnam.utilities.DFUtils._
    expectedDfs.foreach {
      case (name, df) =>
        assert(df.sameAs(result(name), true))
    }
  }

  "joinSingleTablesToFlatTable" should "join the single tables correctly" in {

    //Given
    val conf = FlatteningConfig.load("", "test")
    val expectedMCO: DataFrame = sqlContext.read.option("mergeSchema", "true").parquet("src/test/resources/flattening/parquet-table/flat_table/MCO/")
    val expectedDCIR: DataFrame = sqlContext.read.option("mergeSchema", "true").parquet("src/test/resources/flattening/parquet-table/flat_table/by_month/DCIR/")
    val configTest = conf.copy(join = conf.join.map(_.copy(inputPath = Some("src/test/resources/flattening/parquet-table/single_table"))))

    //When
    val meta = Flattening.joinSingleTablesToFlatTable(sqlContext, configTest)
    val result = meta.map {
      op =>
        op.outputTable -> sqlContext.read
          .option("mergeSchema", "true")
          .load(op.outputPath + "/" + op.outputTable)
          .toDF
    }.toMap

    //Then
    assert(expectedMCO sameAs result("MCO"))
    assert(expectedDCIR sameAs result("DCIR"))

  }

  "joinSingleTablesToFlatTable" should "join the single tables correctly for MCO with patient tables logic" in {

    //Given
    val conf = FlatteningConfig.load("", "test_PMSI")
    val expectedDF = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/" +
      "PMSI_Flat/PMSI_flat.parquet")
    val expectedDF_cols = expectedDF.columns.toList //.sorted.filter(column => !(column.contains("GHM")))
    val expectedDF_sorted = expectedDF.select(expectedDF_cols.map(col): _*)
    val configTest = conf.copy(join = conf.join.map(_.copy(inputPath = Some("src/test/resources/flattening/parquet-table/single_table_PMSI_SSR"))))

    //When
    val meta = Flattening.joinSingleTablesToFlatTable(sqlContext, configTest)
    val result = meta.map {
      op =>
        op.outputTable -> sqlContext.read
          .option("mergeSchema", "true")
          .load(op.outputPath + "/" + op.outputTable)
          .toDF
    }.toMap

    val resultMCO = result("MCO")
    var result_cols = resultMCO.columns.toList.sorted
    result_cols = result_cols //.filter(column => !(column.contains("GHM")))
    val result_sorted = resultMCO.select(result_cols.map(col): _*)

    //Then
    assert(expectedDF_sorted.columns.toSet == result_sorted.columns.toSet)
    assert(expectedDF_sorted.count() == result_sorted.count())

  }

  "joinSingleTablesToFlatTablePMSI" should "join the single tables correctly for MCO with patient tables logic" in {

    //Given
    val conf = FlatteningConfig.load("", "test_PMSI")
    val expectedDF = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/" +
      "PMSI_Flat/PMSI_flat.parquet")
    val expectedDF_cols = expectedDF.columns.toList //.sorted.filter(column => !(column.contains("GHM")))
    val expectedDF_sorted = expectedDF.select(expectedDF_cols.map(col): _*)
    val configTest = conf.copy(join = conf.join.map(_.copy(inputPath = Some("src/test/resources/flattening/parquet-table/single_table_PMSI_SSR"))))

    //When
    val meta = Flattening.joinSingleTablesToFlatTable(sqlContext, configTest)
    val result = meta.map {
      op =>
        op.outputTable -> sqlContext.read
          .option("mergeSchema", "true")
          .load(op.outputPath + "/" + op.outputTable)
          .toDF
    }.toMap

    val resultMCO = result("MCO")
    var result_cols = resultMCO.columns.toList.sorted
    result_cols = result_cols //.filter(column => !(column.contains("GHM")))
    val result_sorted = resultMCO.select(result_cols.map(col): _*)

    //Then
    assert(expectedDF_sorted.columns.toSet == result_sorted.columns.toSet)
    assert(expectedDF_sorted.count() == result_sorted.count())

  }

  "joinSingleTablesToFlatTableSSR" should "join the single tables correctly for SSR with patient tables logic" in {

    //Given
    val conf = FlatteningConfig.load("", "ssr")
    val expectedDF = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/" +
      "SSR_Flat")
    val expectedDF_cols = expectedDF.columns.toList //.sorted.filter(column => !(column.contains("GHM")))
    val expectedDF_sorted = expectedDF.select(expectedDF_cols.map(col): _*)
    val configTest = conf.copy(join = conf.join.map(_.copy(inputPath = Some("src/test/resources/flattening/parquet-table/single_table_PMSI_SSR"))))

    //When
    val meta = Flattening.joinSingleTablesToFlatTable(sqlContext, configTest)
    val result = meta.map {
      op =>
        op.outputTable -> sqlContext.read
          .option("mergeSchema", "true")
          .load(op.outputPath + "/" + op.outputTable)
          .toDF
    }.toMap

    val resultSSR = result("SSR")
    var result_cols = resultSSR.columns.toList.sorted
    result_cols = result_cols //.filter(column => !(column.contains("GHM")))
    val result_sorted = resultSSR.select(result_cols.map(col): _*)

    //Then
    assert(expectedDF_sorted.columns.toSet == result_sorted.columns.toSet)
    assert(expectedDF_sorted.count() == result_sorted.count())

  }

  "joinDCIRAndIR_PHA_R" should "join dcir single tables first and then join ref table IR_PHA_R" in {
    //Given
    val confPath = getClass.getResource("/flattening/config/test/test-ref-join.conf").getPath
    val conf = FlatteningConfig.load(confPath, "test")
    val configTest = conf.copy(join = conf.join.map(_.copy(inputPath = Some("src/test/resources/flattening/parquet-table/single_table"))))
    val expectedDCIR: DataFrame = sqlContext.read.option("mergeSchema", "true").parquet("src/test/resources/flattening/parquet-table/flat_table/ref_join/DCIR")

    //When
    val meta = Flattening.joinSingleTablesToFlatTable(sqlContext, configTest)
    val result = meta.map {
      op =>
        op.outputTable -> sqlContext.read
          .option("mergeSchema", "true")
          .load(op.outputPath + "/" + op.outputTable)
          .toDF
    }.toMap

    val resultSources = meta.map(op => op.outputTable -> op.sources).toMap

    //Then
    assert(expectedDCIR sameAs result("DCIR"))
    assert(resultSources("DCIR").contains("IR_PHA_R"))
  }


}
