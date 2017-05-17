package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{NumericType, StructField, StructType}

object CustomStatistics {

  implicit class Statistics(val df: DataFrame) {

    def customDescribe(inputColumns: Seq[String], distinctOnly: Boolean = false): DataFrame = {

      def computeAvailableAgg(
          schema: StructType,
          colName: String): DataFrame = df.schema(colName) match {

        case StructField(numericColumn: String, _: NumericType, _, _) =>
          df.select(numericColumn)
              .agg(
                min(numericColumn).cast("string").as("Min"),
                max(numericColumn).cast("string").as("Max"),
                count(numericColumn).cast("long").as("Count"),
                countDistinct(numericColumn).cast("long").as("CountDistinct"),
                sum(numericColumn).cast("double").as("Sum"),
                sumDistinct(numericColumn).cast("double").as("SumDistinct"),
                avg(numericColumn).cast("double").as("Avg")
              ).withColumn("ColName", lit(numericColumn))

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

      val outputDF: DataFrame = inputColumns
        .map(computeAvailableAgg(df.schema, _))
        .reduce(_.union(_))

      if (distinctOnly) {
        outputDF
            .drop("Count", "Sum", "Avg")
      } else {
        outputDF
      }
    }
  }
}
