package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{NumericType, StructType}

object CustomStatistics {

  implicit class Statistics(val df: DataFrame) {

    def customDescribe(inputColumns: Array[String], distinctOnly: Boolean = false): DataFrame = {

      def computeAvailableAgg(schema: StructType,
        colName: String): DataFrame =
        colName match {
          case numericColumn if schema.apply(numericColumn).dataType.isInstanceOf[NumericType] =>
            df.select(numericColumn)
              .agg(
                min(numericColumn) cast "string" as "Min",
                max(numericColumn) cast "string" as "Max",
                count(numericColumn) cast "long" as "Count",
                countDistinct(numericColumn) cast "long" as "CountDistinct",
                sum(numericColumn) cast "string" as "Sum",
                sumDistinct(numericColumn) cast "string" as "SumDistinct",
                avg(numericColumn) cast "string" as "Avg"
              ).withColumn("ColName", lit(numericColumn))

          case _ =>
            df.select(colName)
              .agg(
                min(colName) cast "string" as "Min",
                max(colName) cast "string" as "Max",
                count(colName) cast "long" as "Count",
                countDistinct(colName) cast "long" as "CountDistinct"
              ).withColumn("Sum", lit("NA"))
              .withColumn("SumDistinct", lit("NA"))
              .withColumn("Avg", lit("NA"))
              .withColumn("ColName", lit(colName))
        }

      val outputDF: DataFrame = inputColumns
        .map(computeAvailableAgg(df.schema, _))
        .reduce(_.union(_))

      if (distinctOnly) {
        outputDF
          .drop("Count")
          .drop("Sum")
          .drop("Avg")
      }
      else
        outputDF
    }
  }
}
