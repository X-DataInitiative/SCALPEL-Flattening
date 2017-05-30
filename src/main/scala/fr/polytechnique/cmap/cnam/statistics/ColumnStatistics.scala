package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{NumericType, StructField}

object ColumnStatistics {

  def describeColumn(df: DataFrame, colName: String): DataFrame = df.schema(colName) match {

    // For numeric type, compute all stats
    case StructField(numericColumn: String, _: NumericType, _, _) =>
      df.select(numericColumn)
          .agg(
            min(numericColumn).cast("string").as("Min"),
            max(numericColumn).cast("string").as("Max"),
            count(numericColumn).cast("long").as("Count"),
            countDistinct(numericColumn).cast("long").as("CountDistinct"),
            round(sum(numericColumn).cast("double"), 4).as("Sum"),
            round(sumDistinct(numericColumn).cast("double"), 4).as("SumDistinct"),
            round(avg(numericColumn).cast("double"), 4).as("Avg")
          ).withColumn("ColName", lit(numericColumn))

    // For other types, compute only min, max and counts
    case _ =>
      df.select(colName)
          .agg(
            min(colName).cast("string").as("Min"),
            max(colName).cast("string").as("Max"),
            count(colName).cast("long").as("Count"),
            countDistinct(colName).cast("long").as("CountDistinct")
          ).withColumn("Sum", lit(null).cast("double"))
          .withColumn("SumDistinct", lit(null).cast("double"))
          .withColumn("Avg", lit(null).cast("double"))
          .withColumn("ColName", lit(colName))
  }
}
