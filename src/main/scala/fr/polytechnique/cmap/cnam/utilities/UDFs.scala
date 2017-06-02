package fr.polytechnique.cmap.cnam.utilities

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object UDFs {
  def parseTimestamp(pattern: String): UserDefinedFunction = udf(Functions.parseTimestamp(pattern) _)
  def parseDate(pattern: String): UserDefinedFunction = udf(Functions.parseDate(pattern) _)
}
