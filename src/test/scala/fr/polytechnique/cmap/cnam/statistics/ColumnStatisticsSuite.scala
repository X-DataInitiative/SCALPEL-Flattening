package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class ColumnStatisticsSuite extends SharedContext {

  lazy val input: DataFrame = {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    Seq(
      (1, "a"),
      (1, "a"),
      (9, "b")
    ).toDF("numeric", "non-numeric")
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
    val result = ColumnStatistics.describeColumn(input, colName)

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
      ("a", "b", 3L, 2L, None: Option[Double], None: Option[Double], None: Option[Double], "non-numeric", "StringType")
    ).toDF("Min", "Max", "Count", "CountDistinct", "Sum", "SumDistinct", "Avg", "ColName", "ColType")

    // When
    val result = ColumnStatistics.describeColumn(input, colName)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expected)
  }
}
