package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.mockito.Mockito._
import com.typesafe.config.ConfigValueFactory
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils._

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
      (acc: DataFrame, toJoin:DataFrame) =>
        acc.join(toJoin, mockTable.foreignKeys, "inner"))

    // Then
    val result = mockTable.joinByYear(2015)
    assert(result.getDF sameAs expectedJoin)
  }

  "flatTablePerYear" should "return correct flattened Table" in {

    // Given
    val sqlCtx = sqlContext
    val parquetTablesPath = "src/test/resources/flattening/parquet-table"
    val expectedDfPath = "src/test/resources/flattening/MCO"
    val expectedDf = sqlCtx.read.parquet(expectedDfPath)
    val configTest = FlatteningConfig
      .joinTablesConfig
      .head
      .withValue("input_path", ConfigValueFactory.fromAnyRef(parquetTablesPath))
    val flattenedTableTest = new FlatTable(sqlCtx, configTest)

    // When
    val result = flattenedTableTest.flatTablePerYear

    // Then
    assert(
      result.filter(x => x.name.contains("2006")).head.getDF
        sameAs
        expectedDf.filter(col("year") === 2006)
    )
  }

  "WriteAsParquet" should "write results in the correct path" in {

    // Given
    val sqlCtx = sqlContext
    val parquetTablesPath = "src/test/resources/flattening/parquet-table"
    val configTest = FlatteningConfig
      .joinTablesConfig
      .head
      .withValue("input_path", ConfigValueFactory.fromAnyRef(parquetTablesPath))
    val flattenedTableTest = new FlatTable(sqlCtx, configTest)
    val resultPath = "target/test/output/join"
    val expectedDf = sqlCtx.read.parquet("src/test/resources/flattening/MCO")
    // When
    flattenedTableTest.writeAsParquet
    val result =  sqlCtx.read.parquet(resultPath)
    // Then
    assert(resultPath == flattenedTableTest.outputBasePath)
    assert(result sameAs expectedDf)
  }

}
