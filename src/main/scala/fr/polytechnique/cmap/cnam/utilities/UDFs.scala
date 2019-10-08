// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.utilities

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

object UDFs {
  def parseTimestamp(pattern: String): UserDefinedFunction = udf {
    v: String => Functions.parseTimestamp(v, pattern)
  }
  def parseDate(pattern: String): UserDefinedFunction = udf {
    v: String => Functions.parseDate(v, pattern)
  }
}
