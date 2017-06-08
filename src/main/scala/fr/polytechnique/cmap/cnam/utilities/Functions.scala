package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp
import java.text.{ParsePosition, SimpleDateFormat}

object Functions {

  class DateParseException(val msg: String) extends Exception(msg)

  def parseTimestamp(value: String, pattern: String = "yyyy-MM-dd HH:mm:ss.S"): Option[Timestamp] = {

    if (value == null || value.trim == "") return None

    val dateFormat = new SimpleDateFormat(pattern)
    // Only exact patterns should be accepted
    // https://docs.oracle.com/javase/8/docs/api/java/text/DateFormat.html#setLenient-boolean-
    dateFormat.setLenient(false)

    Option(dateFormat.parse(value.trim, new ParsePosition(0))) match {
      case None => throw new DateParseException(
        s"""Cannot parse Timestamp with pattern "$pattern" from the string value "$value""""
      )
      case Some(date: java.util.Date) => Some(new Timestamp(date.getTime))
    }
  }

  def parseDate(value: String, pattern: String = "yyyy-MM-dd"): Option[java.sql.Date] = {
    parseTimestamp(value, pattern).map(ts => new java.sql.Date(ts.getTime))
  }
}
