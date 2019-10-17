// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.utilities

import java.sql.{Date, Timestamp}
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

  implicit class IntFunctions(num: Int) {
    def between(lower: Int, upper: Int): Boolean = num >= lower && num <= upper
  }

  implicit def timestampToDate(ts: Timestamp): Date = new Date(ts.getTime)

  def makeTS(year: Int, month: Int, day: Int, hour: Int = 0, minute: Int = 0, second: Int = 0): Timestamp = {
    if(!year.between(1000, 3000) || !month.between(1, 12) || !day.between(1,31) ||
        !hour.between(0, 23) || !minute.between(0, 59) || !second.between(0,59))
      throw new java.lang.IllegalArgumentException("Out of bounds.")

    Timestamp.valueOf(f"$year%04d-$month%02d-$day%02d $hour%02d:$minute%02d:$second%02d")
  }

  def makeTS(timestampParam: List[Integer]): Timestamp = timestampParam match {

    case List(year, month, day) => makeTS(year, month, day)
    case List(year, month, day, hour) => makeTS(year, month, day, hour)
    case List(year, month, day, hour, minute) => makeTS(year, month, day, hour, minute)
    case List(year, month, day, hour, minute, second) => makeTS(year, month, day, hour, minute, second)
    case _ => throw new IllegalArgumentException("Illegal Argument List for makeTS function")
  }
}
