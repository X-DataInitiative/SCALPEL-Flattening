package fr.polytechnique.cmap.cnam.utilities

import java.sql.Timestamp
import org.scalatest.FlatSpec

class FunctionsSuite extends FlatSpec {

  "parseTimestamp" should "convert a string to a Timestamp if it's not null and non-empty" in {

    // Given
    val input1 = "1991-03-15 18:21:00.000"
    val input2 = "15/03/91 18:21"
    val pattern2 = "dd/MM/yy HH:mm"
    val expected = Some(Timestamp.valueOf("1991-03-15 18:21:00"))

    // When
    val result1 = Functions.parseTimestamp(input1)
    val result2 = Functions.parseTimestamp(input2, pattern2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }

  it should "return None if the input string is empty or null" in {

    // Given
    val input1: String = """
      """
    val input2: String = null
    val expected: Option[Timestamp] = None

    // When
    val result1 = Functions.parseTimestamp(input1)
    val result2 = Functions.parseTimestamp(input2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }

  it should "throw an exception if parsing is not possible" in {

    // Given
    val pattern = "dd/MM/yyyy HH:mm"
    val input = "01/15/2010 23:59"

    // WhenThen
    intercept[Functions.DateParseException] {
      Functions.parseTimestamp(input, pattern)
    }
  }

  "parseDate" should "convert a string to a java.sql.Date if it's not null and non-empty" in {

    // Given
    val input1 = "1991-03-15"
    val input2 = "15/03/91"
    val pattern2 = "dd/MM/yy"
    val expected = Some(java.sql.Date.valueOf("1991-03-15"))

    // When
    val result1 = Functions.parseDate(input1)
    val result2 = Functions.parseDate(input2, pattern2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }

  it should "return None if the input string is empty or null" in {

    // Given
    val input1: String = """
                         """
    val input2: String = null
    val expected: Option[java.sql.Date] = None

    // When
    val result1 = Functions.parseDate(input1)
    val result2 = Functions.parseDate(input2)

    // Then
    assert(result1 == expected)
    assert(result2 == expected)
  }


  it should "throw an exception if parsing is not possible" in {
    // Given
    val pattern = "dd/MM/yyyy"
    val input = "01/15/2010"

    // WhenThen
    intercept[Functions.DateParseException] {
      Functions.parseDate(input, pattern)
    }
  }

}
