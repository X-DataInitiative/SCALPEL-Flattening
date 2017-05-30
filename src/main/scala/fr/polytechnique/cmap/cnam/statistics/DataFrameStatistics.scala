package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame

object DataFrameStatistics {

  implicit class Statistics(val df: DataFrame) {

    def customDescribe(
        colNames: Seq[String] = df.columns,
        distinctOnly: Boolean = false): DataFrame = {

      val outputDF: DataFrame = colNames
        .map(colName => ColumnStatistics.describeColumn(df, colName))
        .reduce(_.union(_))

      if (distinctOnly) outputDF.drop("Count", "Sum", "Avg")
      else outputDF
    }
  }
}
