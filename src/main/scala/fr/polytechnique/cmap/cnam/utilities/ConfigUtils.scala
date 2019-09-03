package fr.polytechnique.cmap.cnam.utilities

import scala.util.Try
import org.apache.spark.network.util.JavaUtils

object ConfigUtils {

  /**
    * Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for internal use.
    *
    * If no suffix is provided, the passed number is assumed to be in bytes.
    */
  def byteStringAsBytes(str: String): Long = {
    {
      Try(JavaUtils.byteStringAsBytes(str)) recover {
        case _: NumberFormatException => -1L // input a invalid value return -1
      }
    }.get
  }

}
