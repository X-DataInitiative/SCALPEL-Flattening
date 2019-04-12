package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp


package object functions {

  def makeTS(year: Int, month: Int, day: Int, hour: Int = 0, minute: Int = 0, second: Int = 0): Timestamp = {
    if(!year.between(1000, 3000) || !month.between(1, 12) || !day.between(1,31) ||
      !hour.between(0, 23) || !minute.between(0, 59) || !second.between(0,59))
      throw new java.lang.IllegalArgumentException("Out of bounds.")

    Timestamp.valueOf(f"$year%04d-$month%02d-$day%02d $hour%02d:$minute%02d:$second%02d")
  }

  def makeTS(timestampParam: List[Integer]): Timestamp = timestampParam match {

    case List(year, month, day) => makeTS(year, month, day)
    case _ => throw new IllegalArgumentException("Illegal Argument List for makeTS function")
  }

  implicit class IntFunctions(num: Int) {
    def between(lower: Int, upper: Int): Boolean = num >= lower && num <= upper
  }
}
