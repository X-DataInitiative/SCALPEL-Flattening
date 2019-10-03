package fr.polytechnique.cmap.cnam.flattening

import org.mockito.Mockito._
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.{Reference, YearAndMonths}
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import fr.polytechnique.cmap.cnam.utilities.Functions._

class FlatTableSuite extends SharedContext {

  "joinByYear" should "return correct results" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val mockTable = mock(classOf[FlatTable])
    val central: DataFrame = Seq(
      (1, Some("hello1"), 2014),
      (2, Some("hello2"), 2015)
    ).toDF("key", "value", "year")
    val tableName: String = "central"
    val other1: DataFrame = Seq(
      (1, Some("world1"), 2014),
      (2, Some("world2"), 2015)
    ).toDF("key", "valueOther1", "year")
    val other2: DataFrame = Seq(
      (1, Some("other1"), 2014),
      (2, Some("other2"), 2015)
    ).toDF("key", "valueOther2", "year")

    val simpleTable = new Table(tableName, central)
    val otherTable1 = new Table("other1", other1)
    val otherTable2 = new Table("other2", other2)

    val expectedJoin = Seq(
      (2, Some("hello2"), 2015, Some("world2"), Some("other2"))
    ).toDF("key", "value", "year", "other1__valueOther1", "other2__valueOther2")

    // When
    when(mockTable.mainTable).thenReturn(simpleTable)
    when(mockTable.tablesToJoin).thenReturn(List(otherTable1, otherTable2))
    when(mockTable.foreignKeys).thenReturn(List("key"))
    when(mockTable.joinByYear(2015)).thenCallRealMethod()
    when(mockTable.joinFunction).thenReturn(
      (acc: DataFrame, toJoin: DataFrame) =>
        acc.join(toJoin, mockTable.foreignKeys, "left_outer"))

    // Then
    val result = mockTable.joinByYear(2015)
    assert(result.df sameAs expectedJoin)
  }

  "joinRefs" should "join flat table and references" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mockTable = mock(classOf[FlatTable])
    val central = Seq(
      ("01234", "3400935418487", 2014),
      ("56789", "3400935563538", 2014)
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "year")
    val centralTable = new Table("central", central)
    val ref = Seq(
      ("3400935418487", "GLICLAZIDE"),
      ("3400935563538", "PIOGLITAZONE")
    ).toDF("PHA_CIP_C13", "molecule_combination")
    val refTable = new Table("reference", ref)
    val config = Reference(name = "reference", joinKeys = List(List("ER_PHA_F__PHA_PRS_C13", "reference__PHA_CIP_C13")))
    val expected = Seq(
      ("01234", "3400935418487", 2014, "3400935418487", "GLICLAZIDE"),
      ("56789", "3400935563538", 2014, "3400935563538", "PIOGLITAZONE")
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "year", "reference__PHA_CIP_C13", "reference__molecule_combination")

    //when
    when(mockTable.referencesToJoin).thenReturn(List((refTable, config)))
    when(mockTable.joinRefs(centralTable)).thenCallRealMethod()

    //then
    val result = mockTable.joinRefs(centralTable)
    assert(result.name == "central")
    assertDFs(expected, result.df)
  }

  "joinByYearAndDate" should "return correct results" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val mockTable = mock(classOf[FlatTable])
    val central: DataFrame = Seq(
      (1, Some("hello1"), 2014, makeTS(2014, 2, 5)),
      (2, Some("hello2"), 2015, makeTS(2015, 1, 15))
    ).toDF("key", "value", "year", "date")
    val tableName: String = "central"
    val other1: DataFrame = Seq(
      (1, Some("world1"), 2014, makeTS(2014, 2, 5)),
      (2, Some("world2"), 2015, makeTS(2015, 1, 15))
    ).toDF("key", "valueOther1", "year", "date")
    val other2: DataFrame = Seq(
      (1, Some("other1"), 2014, makeTS(2014, 2, 5)),
      (2, Some("other2"), 2015, makeTS(2015, 1, 15))
    ).toDF("key", "valueOther2", "year", "date")

    val simpleTable = new Table(tableName, central)
    val otherTable1 = new Table("other1", other1)
    val otherTable2 = new Table("other2", other2)

    val expectedJoin = Seq(
      (2, makeTS(2015, 1, 15), Some("hello2"), 2015, Some("world2"), Some("other2"))
    ).toDF("key", "date", "value", "year", "other1__valueOther1", "other2__valueOther2")

    // When
    when(mockTable.mainTable).thenReturn(simpleTable)
    when(mockTable.tablesToJoin).thenReturn(List(otherTable1, otherTable2))
    when(mockTable.foreignKeys).thenReturn(List("key", "date"))
    when(mockTable.joinByYearAndDate(2015, 1, "date")).thenCallRealMethod()
    when(mockTable.joinFunction).thenReturn(
      (acc: DataFrame, toJoin: DataFrame) =>
        acc.join(toJoin, mockTable.foreignKeys, "left_outer"))

    // Then
    val result = mockTable.joinByYearAndDate(2015, 1, "date")
    assert(result.df sameAs expectedJoin)
  }

  "flatTablePerYear" should "return correct list of years" in {

    // Given
    val conf = FlatteningConfig.load("", "test")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val expected = Array(2008, 2007, 2006)
    val configTest = conf.joinTableConfigs.head.copy(inputPath = Some(parquetTablesPath))
    val flattenedTableTest = new FlatTable(sqlContext, configTest)

    // When
    val result = flattenedTableTest.flatTablePerYear

    // Then
    assert(expected === result)
  }

  "WriteAsParquet" should "flatten MCO and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "test")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val configTest = conf.joinTableConfigs.head.copy(inputPath = Some(parquetTablesPath))
    val flattenedTableTest = new FlatTable(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDf = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/MCO")


    // When
    flattenedTableTest.writeAsParquet()
    val result = sqlContext.read.parquet(resultPath)

    // Then
    assert(resultPath == flattenedTableTest.outputBasePath)
    assert(result sameAs expectedDf)
  }

  "WriteAsParquet" should "flatten DCIR and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "test")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val configTest = conf.joinTableConfigs.tail.head.copy(inputPath = Some(parquetTablesPath))
    val flattenedTableTest = new FlatTable(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDf = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/by_month/DCIR")
    // When
    flattenedTableTest.writeAsParquet()
    val result = sqlContext.read.parquet(resultPath)
    // Then
    assert(resultPath == flattenedTableTest.outputBasePath)
    assert(result sameAs expectedDf)
  }

  "writeAsParquet in particular month" should "flatten DCIR and write some months of data specified in conf" in {
    // Given
    val conf = FlatteningConfig.load("", "test")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val dcirConf = conf.joinTableConfigs.tail.head.copy(inputPath = Some(parquetTablesPath))
    val configTest = dcirConf.copy(onlyOutput = List(YearAndMonths(2006, List(3))))
    val flattenedTableTest = new FlatTable(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDf = sqlContext.read
      .parquet("src/test/resources/flattening/parquet-table/flat_table/by_month/DCIR")
      .filter("month=3")
    // When
    flattenedTableTest.writeAsParquet()
    val result = sqlContext.read.parquet(resultPath)
    // Then
    assert(resultPath == flattenedTableTest.outputBasePath)
    assert(result sameAs expectedDf)

  }

  "write table" should "write the df in the correct path" in {


    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val mockTable = mock(classOf[FlatTable])
    val central: DataFrame = Seq(
      (1, Some("hello1"), 2014, makeTS(2014, 2, 5)),
      (2, Some("hello2"), 2015, makeTS(2015, 1, 15))
    ).toDF("key", "value", "year", "date")
    val tableName: String = "central"
    val simpleTable = new Table(tableName, central)
    val path = "target/test/output/join"

    // When
    when(mockTable.mainTable).thenReturn(simpleTable)
    when(mockTable.logger).thenCallRealMethod()
    when(mockTable.writeTable(simpleTable)).thenCallRealMethod()
    when(mockTable.outputBasePath).thenReturn(path)
    when(mockTable.saveMode).thenReturn("append")
    mockTable.writeTable(simpleTable)

    // Then
    val result = sqlCtx.read.parquet(path + "/" + tableName)
    assert(result sameAs simpleTable.df)
  }
}
