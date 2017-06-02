package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp
import java.text.SimpleDateFormat

object Functions {

  def parseTimestamp(pattern: String)(value: String): Option[Timestamp] = {

    if (value.trim() == "" || value == null) return None

    Option(new SimpleDateFormat(pattern).parse(value)) match {
      case None => throw new Exception(s"""Cannot parse a timestamp with pattern "$pattern" from the string value "$value"""")
      case Some(date: java.util.Date) => Some(new Timestamp(date.getTime))
    }
  }

  def parseDate(pattern: String)(value: String): Option[java.sql.Date] = {

    if (value.trim() == "" || value == null) return None

    Option(new SimpleDateFormat(pattern).parse(value)) match {
      case None => throw new Exception(s"""Cannot parse a timestamp with pattern "$pattern" from the string value "$value"""")
      case Some(date: java.util.Date) => Some(new java.sql.Date(date.getTime))
    }
  }
}
