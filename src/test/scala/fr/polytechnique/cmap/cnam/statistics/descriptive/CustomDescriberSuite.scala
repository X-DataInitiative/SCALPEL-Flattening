// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.statistics.descriptive

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class CustomDescriberSuite extends SharedContext {

  lazy val simpleInput: DataFrame = {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    Seq(
      (1, "a"),
      (1, "a"),
      (9, "b")
    ).toDF("numeric", "non-numeric")
  }

  lazy val irBen: DataFrame = {
    val srcFilePath: String = "src/test/resources/statistics/custom-statistics/IR_BEN_R.csv"
    sqlContext.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(srcFilePath)
  }

  "describeColumn" should "return a DataFrame containing the statistics for a numeric column" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = "numeric"
    val expected = Seq(
      ("1", "9", 3L, 2L, 11D, 10D, 3.6667D, "numeric", "IntegerType")
    ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName", "ColType")

    // When
    import CustomDescriber._
    val result = simpleInput.describeColumn(colName)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expected)
  }

  it should "return a DataFrame containing the statistics for a non-numeric column" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val colName = "non-numeric"
    val expected = Seq(
      ("a", "b", 3L, 2L, None: Option[Double], None: Option[Double], None: Option[Double],
        "non-numeric", "StringType")
    ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName", "ColType")

    // When
    import CustomDescriber._
    val result = simpleInput.describeColumn(colName)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expected)
  }

  "customDescriber" should "compute statistics on all columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf = irBen
      .select("BEN_CDI_NIR", "BEN_DTE_MAJ", "BEN_SEX_COD", "MAX_TRT_DTD", "ORG_CLE_NEW", "NUM_ENQ")
    val expected: DataFrame = Seq(
      ("0", "0", 2L, 1L, Some(0.0), Some(0.0), Some(0.0), "BEN_CDI_NIR", "IntegerType"),
      ("01/01/2006", "25/01/2006", 2L, 2L, null, null, null, "BEN_DTE_MAJ", "StringType"),
      ("1", "2", 2L, 2L, Some(3.0), Some(3.0), Some(1.5), "BEN_SEX_COD", "IntegerType"),
      ("07/03/2008", "07/03/2008", 1L, 1L, null, null, null, "MAX_TRT_DTD", "StringType"),
      ("CODE1234", "CODE1234", 2L, 1L, null, null, null, "ORG_CLE_NEW", "StringType"),
      ("Patient_01", "Patient_02", 2L, 2L, null, null, null, "NUM_ENQ", "StringType")
    ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName", "ColType")

    // When
    import CustomDescriber._
    val result = inputDf.customDescribe(inputDf.columns)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(expected sameAs result)
  }

  it should "compute only disctinct statistics when distinct only flag is set" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf = irBen
      .select("BEN_CDI_NIR", "BEN_DTE_MAJ", "BEN_SEX_COD", "MAX_TRT_DTD", "ORG_CLE_NEW", "NUM_ENQ")
    val expected: DataFrame = Seq(
      ("0", "0", 1L, Some(0.0), "BEN_CDI_NIR", "IntegerType"),
      ("01/01/2006", "25/01/2006", 2L, null, "BEN_DTE_MAJ", "StringType"),
      ("1", "2", 2L, Some(3.0), "BEN_SEX_COD", "IntegerType"),
      ("07/03/2008", "07/03/2008", 1L, null, "MAX_TRT_DTD", "StringType"),
      ("CODE1234", "CODE1234", 1L, null, "ORG_CLE_NEW", "StringType"),
      ("Patient_01", "Patient_02", 2L, null, "NUM_ENQ", "StringType")
    ).toDF("Min", "Max", "CountDistinct", "SumDistinct", "ColName", "ColType")

    // When
    import CustomDescriber._
    val result = inputDf.customDescribe(inputDf.columns, distinctOnly = true)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(expected sameAs result)
  }

  it should "compute statistics on specified columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf = irBen
    val inputColumns = Seq("BEN_TOP_CNS", "BEN_DCD_DTE", "NUM_ENQ")
    val expected = Seq(
      ("1", "1", 2L, 1L, Some(2.0), Some(1.0), Some(1.0), "BEN_TOP_CNS", "IntegerType"),
      ("25/01/2008", "25/01/2008", 1L, 1L, null, null, null, "BEN_DCD_DTE", "StringType"),
      ("Patient_01", "Patient_02", 2L, 2L, null, null, null, "NUM_ENQ", "StringType")
    ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName", "ColType")

    // When
    import CustomDescriber._
    val resultColumns = inputDf.customDescribe(inputColumns)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(resultColumns sameAs expected)
  }

  it should "throw an exception" in {

    // Given
    val givenDF = irBen
    val invalidCols = Seq("NUM_ENQ", "INVALID_COLUMN")

    // When
    import CustomDescriber._
    val thrown = intercept[java.lang.IllegalArgumentException] {
      givenDF.customDescribe(invalidCols).count
    }

    // Then
    assert(thrown.getMessage.matches("Field \"[^\"]*\" does not exist."))
  }

}
