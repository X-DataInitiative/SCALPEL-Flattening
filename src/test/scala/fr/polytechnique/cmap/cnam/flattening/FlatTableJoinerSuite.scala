// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.mockito.Mockito._
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.{Reference, YearAndMonths}
import fr.polytechnique.cmap.cnam.flattening.join.FlatTableJoiner
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import fr.polytechnique.cmap.cnam.utilities.Functions._

class FlatTableJoinerSuite extends SharedContext {

  "joinByYear" should "return correct results" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val mockTable = mock(classOf[FlatTableJoiner])
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

    val simpleTable = TestTable(tableName, central)
    val otherTable1 = TestTable("other1", other1)
    val otherTable2 = TestTable("other2", other2)

    val expectedJoin = Seq(
      (2, Some("hello2"), 2015, Some("world2"), Some("other2"))
    ).toDF("key", "value", "year", "other1__valueOther1", "other2__valueOther2")

    // When
    when(mockTable.mainTable).thenReturn(simpleTable)
    when(mockTable.foreignKeys).thenReturn(List("key"))
    when(mockTable.tablesToJoin).thenReturn(List(otherTable1, otherTable2))
    when(mockTable.joinByYear("mock", 2015)).thenCallRealMethod()
    when(mockTable.joinFunction).thenReturn(
      (acc: DataFrame, toJoin: DataFrame) =>
        acc.join(toJoin, List("key"), "left_outer"))

    // Then
    val result = mockTable.joinByYear("mock", 2015)
    assert(result.df sameAs expectedJoin)
  }

  "joinRefs" should "join flat table and references" in {
    //Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    val mockTable = mock(classOf[FlatTableJoiner])
    val central = Seq(
      ("01234", "3400935418487", 2014),
      ("56789", "3400935563538", 2014)
    ).toDF("NUM_ENQ", "ER_PHA_F__PHA_PRS_C13", "year")
    val centralTable = TestTable("central", central)
    val ref = Seq(
      ("3400935418487", "GLICLAZIDE"),
      ("3400935563538", "PIOGLITAZONE")
    ).toDF("PHA_CIP_C13", "molecule_combination")
    val refTable = TestTable("reference", ref)
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

    val mockTable = mock(classOf[FlatTableJoiner])
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

    val simpleTable = TestTable(tableName, central)
    val otherTable1 = TestTable("other1", other1)
    val otherTable2 = TestTable("other2", other2)

    val expectedJoin = Seq(
      (2, makeTS(2015, 1, 15), Some("hello2"), 2015, Some("world2"), Some("other2"))
    ).toDF("key", "date", "value", "year", "other1__valueOther1", "other2__valueOther2")

    // When
    when(mockTable.mainTable).thenReturn(simpleTable)
    when(mockTable.foreignKeys).thenReturn(List("key", "date"))
    when(mockTable.tablesToJoin).thenReturn(List(otherTable1, otherTable2))
    when(mockTable.joinByYearAndDate("mock", 2015, 1, "date")).thenCallRealMethod()
    when(mockTable.joinFunction).thenReturn(
      (acc: DataFrame, toJoin: DataFrame) =>
        acc.join(toJoin, List("key", "date"), "left_outer"))

    // Then
    val result = mockTable.joinByYearAndDate("mock", 2015, 1, "date")
    assert(result.df sameAs expectedJoin)
  }

  "WriteAsParquet" should "flatten MCO and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "test")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val configTest = conf.joinTableConfigs.head.copy(inputPath = Some(parquetTablesPath))
    val flattenedTableTest = new FlatTableJoiner(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDf = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/MCO")


    // When
    flattenedTableTest.join
    val result = sqlContext.read.parquet(resultPath)

    // Then
    assert(result sameAs expectedDf)
  }

  "WriteAsParquet" should "flatten DCIR and write it in the correct path" in {

    // Given
    val conf = FlatteningConfig.load("", "test")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val configTest = conf.joinTableConfigs.tail.head.copy(inputPath = Some(parquetTablesPath))
    val flattenedTableTest = new FlatTableJoiner(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDf = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/by_month/DCIR")
    // When
    flattenedTableTest.join
    val result = sqlContext.read.parquet(resultPath)
    // Then
    assert(result sameAs expectedDf)
  }

  "writeAsParquet in particular month" should "flatten DCIR and write some months of data specified in conf" in {
    // Given
    val conf = FlatteningConfig.load("", "test")
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val dcirConf = conf.joinTableConfigs.tail.head.copy(inputPath = Some(parquetTablesPath))
    val configTest = dcirConf.copy(onlyOutput = List(YearAndMonths(2006, List(3))))
    val flattenedTableTest = new FlatTableJoiner(sqlContext, configTest)
    val resultPath = conf.flatTablePath
    val expectedDf = sqlContext.read
      .parquet("src/test/resources/flattening/parquet-table/flat_table/by_month/DCIR")
      .filter("month=3")
    // When
    flattenedTableTest.join
    val result = sqlContext.read.parquet(resultPath)
    // Then
    assert(result sameAs expectedDf)

  }

  "write table" should "write the df in the correct path" in {


    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val mockTable = mock(classOf[FlatTableJoiner])
    val central: DataFrame = Seq(
      (1, Some("hello1"), 2014, makeTS(2014, 2, 5)),
      (2, Some("hello2"), 2015, makeTS(2015, 1, 15))
    ).toDF("key", "value", "year", "date")
    val tableName: String = "central"
    val simpleTable = TestTable(tableName, central)
    val path = "target/test/output/join"

    // When
    when(mockTable.mainTable).thenReturn(simpleTable)
    when(mockTable.logger).thenCallRealMethod()
    when(mockTable.writeTable(simpleTable, path, "overwrite")).thenCallRealMethod()
    mockTable.writeTable(simpleTable, path, "overwrite")

    // Then
    val result = sqlCtx.read.parquet(path + "/" + tableName)
    assert(result sameAs simpleTable.df)
  }
}
