package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class CustomStatisticsSuite extends SharedContext {

  def getSampleDf: DataFrame = {
    val srcFilePath: String = "src/test/resources/statistics/custom-statistics/IR_BEN_R.csv"
    val sampleDf = sqlContext
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ";")
      .csv(srcFilePath)
    sampleDf
  }

  "customDescribe" should "compute statistics on all columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf = getSampleDf
      .select("BEN_CDI_NIR", "BEN_DTE_MAJ", "BEN_SEX_COD", "MAX_TRT_DTD", "ORG_CLE_NEW", "NUM_ENQ")
    val expected: DataFrame = Seq(
        ("0", "0", 2L, 1L, "0", "0", "0.0", "BEN_CDI_NIR"),
        ("01/01/2006", "25/01/2006", 2L, 2L, "NA", "NA", "NA", "BEN_DTE_MAJ"),
        ("1", "2", 2L, 2L, "3", "3", "1.5", "BEN_SEX_COD"),
        ("07/03/2008", "07/03/2008", 1L, 1L, "NA", "NA", "NA", "MAX_TRT_DTD"),
        ("CODE1234", "CODE1234", 2L, 1L, "NA", "NA", "NA", "ORG_CLE_NEW"),
        ("Patient_01", "Patient_02", 2L, 2L, "NA", "NA", "NA", "NUM_ENQ")
      ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName")

    // When
    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._
    val result = inputDf.customDescribe(inputDf.columns)

    // Then
    result.show
    result.printSchema
    expected.show
    expected.printSchema

    import utilities.DFUtils.CSVDataFrame
    assert(expected sameAs result)
  }

  it should "compute only disctinct statistics when distinct only flag is set" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf = getSampleDf
      .select("BEN_CDI_NIR", "BEN_DTE_MAJ", "BEN_SEX_COD", "MAX_TRT_DTD", "ORG_CLE_NEW", "NUM_ENQ")
    val expected: DataFrame = Seq(
      ("0", "0", 1L, "0", "BEN_CDI_NIR"),
      ("01/01/2006", "25/01/2006", 2L, "NA", "BEN_DTE_MAJ"),
      ("1", "2", 2L, "3", "BEN_SEX_COD"),
      ("07/03/2008", "07/03/2008", 1L, "NA", "MAX_TRT_DTD"),
      ("CODE1234", "CODE1234", 1L, "NA", "ORG_CLE_NEW"),
      ("Patient_01", "Patient_02", 2L, "NA", "NUM_ENQ")
    ).toDF("Min", "Max", "CountDistinct", "SumDistinct", "ColName")

    // When
    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._
    val result = inputDf.customDescribe(inputDf.columns, distinctOnly = true)

    // Then
    result.show
    result.printSchema
    expected.show
    expected.printSchema

    import utilities.DFUtils.CSVDataFrame
    assert(expected sameAs result)
  }

  it should "compute statistics on specified columns" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    // Given
    val inputDf = getSampleDf
    val inputColumns = Array("BEN_TOP_CNS", "BEN_DCD_DTE", "NUM_ENQ")
    val expected = {
      Seq(
        ("1", "1", 2L, 1L, "2", "1", "1.0", "BEN_TOP_CNS"),
        ("25/01/2008", "25/01/2008", 1L, 1L, "NA", "NA", "NA", "BEN_DCD_DTE"),
        ("Patient_01", "Patient_02", 2L, 2L, "NA", "NA", "NA", "NUM_ENQ")
      ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName")
    }

    // When
    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._
    val resultColumns = inputDf.customDescribe(inputColumns)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(resultColumns sameAs expected)
  }

  it should "throw an exception" in {

    // Given
    val givenDF = getSampleDf
    val invalidCols = Array("NUM_ENQ", "INVALID_COLUMN")

    // When
    import fr.polytechnique.cmap.cnam.statistics.CustomStatistics._
    val thrown = intercept[java.lang.IllegalArgumentException] {
      givenDF.customDescribe(invalidCols).count
    }

    // Then
    assert(thrown.getMessage.matches("Field \"[^\"]*\" does not exist."))
  }

}
