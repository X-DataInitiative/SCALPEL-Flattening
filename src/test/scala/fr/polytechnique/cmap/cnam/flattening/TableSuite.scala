package fr.polytechnique.cmap.cnam.flattening
import org.apache.spark.sql.DataFrame
import com.typesafe.config.ConfigValueFactory
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils._


class TableSuite extends SharedContext {

  "addPrefix" should "add a prefix" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf: DataFrame = Seq(
      (Some("1"), Some("5"), None, "2008-01-01"),
      (Some("2"), None, Some("3"), "2008-05-03"),
      (Some("3"), Some("2"), Some("1"), "2009-02-02"),
      (Some("4"), None, None, "2005-05-03"),
      (Some("5"), Some("2"), Some("1"), "2005-03-06")
    ).toDF("PatientID", "codep", "dab", "date")
    val inputTable = new Table("inputDf", inputDf)
    val expected: DataFrame = Seq(
      (Some("1"), Some("5"), None, "2008-01-01"),
      (Some("2"), None, Some("3"), "2008-05-03"),
      (Some("3"), Some("2"), Some("1"), "2009-02-02"),
      (Some("4"), None, None, "2005-05-03"),
      (Some("5"), Some("2"), Some("1"), "2005-03-06")
    ).toDF("PatientID", "central__codep", "central__dab", "central__date")

    // When
    import inputTable.TableHelper
    val result = inputDf.addPrefix("central",List("PatientID"))

    // Then
    assert(result sameAs expected)

  }

  "getYears" should "extract years" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val inputDf: DataFrame = Seq(
      (Some("1"), Some("5"), None, 2008),
      (Some("2"), None, Some("3"), 2008),
      (Some("3"), Some("2"), Some("1"), 2009),
      (Some("4"), None, None, 2005),
      (Some("5"), Some("2"), Some("1"), 2005)
    ).toDF("PatientID", "codep", "dab", "year")
    val inputTable = new Table("input", inputDf)
    val expected = List(2005, 2008, 2009)

    // When
    val result = inputTable.getYears

    // Then
    assert(result.sorted === expected.sorted)
  }

  "filterByYear" should "filter table df on the given year correctly" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDf: DataFrame = Seq(
      (Some("1"), Some("5"), None, 2008),
      (Some("2"), None, Some("3"), 2008),
      (Some("3"), Some("2"), Some("1"), 2009),
      (Some("4"), None, None, 2005),
      (Some("5"), Some("2"), Some("1"), 2005)
    ).toDF("PatientID", "codep", "dab", "year")

    val expectedResult: DataFrame = Seq(
      (Some("4"), None, None, 2005),
      (Some("5"), Some("2"), Some("1"), 2005)
    ).toDF("PatientID", "codep", "dab", "year")

    val inputTable = new Table("input", inputDf)
    val filteringYear = 2005

    // When
    val result = inputTable.filterByYear(filteringYear)

    // Then
    assert(result sameAs expectedResult)
  }

  "filterByYearAndAnnotate" should "filter table df on the given year and annotate column names " +
    "correctly" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val inputDf: DataFrame = Seq(
      (Some("1"), Some("5"), None, 2008),
      (Some("2"), None, Some("3"), 2008),
      (Some("3"), Some("2"), Some("1"), 2009),
      (Some("4"), None, None, 2005),
      (Some("5"), Some("2"), Some("1"), 2005)
    ).toDF("PatientID", "codep", "dab", "year")

    val expectedResult: DataFrame = Seq(
      (Some("4"), None, None),
      (Some("5"), Some("2"), Some("1"))
    ).toDF("PatientID", "input__codep", "input__dab")

    val inputTable = new Table("input", inputDf)
    val filteringYear = 2005
    val annotateColumnsExcept = List("PatientID", "year")

    // When
    val result = inputTable.filterByYearAndAnnotate(filteringYear, annotateColumnsExcept)

    // Then
    assert(result sameAs expectedResult)
  }

  "build" should "given a tableName extract the right table from the config" in {

    // Given
    val sqlCtx = sqlContext
    val parquetTablesPath = "src/test/resources/flattening/parquet-table/single_table"
    val tableName = "ER_CAM_F"
    val configTest = FlatteningConfig
      .joinTablesConfig
      .head
      .withValue("input_path", ConfigValueFactory.fromAnyRef(parquetTablesPath))
    val flattenedTableTest = new FlatTable(sqlCtx, configTest)
    val expectedDf = sqlCtx.read.parquet(s"$parquetTablesPath/$tableName")

    // When
    val result = Table.build(sqlCtx, parquetTablesPath, tableName)
    // Then
    assert(result.df sameAs expectedDf)
    assert(result.name === tableName)

  }
}
