// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.scalatest.FlatSpecLike
import fr.polytechnique.cmap.cnam.flattening.convert.CSVSchemaReader

class CSVSchemaReaderSuite extends FlatSpecLike {

  "readSchemaFiles" should "look for the right file and parse it without nullPointerException" in {

    //Given
    val csvPaths = List(
      "flattening/schema/raw/fake_schema.csv",
      "flattening/schema/raw/fake_schema_2.csv"
    )

    //When
    val result: List[String] = CSVSchemaReader.readSchemaFiles(csvPaths)

    //Then
    assert(result.length == 12)
  }

  "readSchemaFile" should "look for the right file and parse it without nullPointerException" in {

    //Given
    val csvPath = "flattening/schema/raw/fake_schema.csv"

    //When
    val result: List[String] = CSVSchemaReader.readSchemaFile(csvPath)

    //Then
    assert(result.length == 6)
  }

  "readColumnsType" should "transform a list of csv into a nice map of columns names and types" in {
    // Given
    val inputLines = List(
      "MEMNAME;NAME;FORMAT;FORMATL;FORMATD;DATATYPE",
      "TABLE1;COLUMN1;$;4;0;TYPE1",
      "TABLE1;COLUMN2;$;4;0;TYPE2",
      "TABLE2;COLUMN1;$;4;0;TYPE3",
      "TABLE2;COLUMN2;$;4;0;TYPE4"
    )

    val expected = Map(
      "TABLE1" -> List(("COLUMN1", "TYPE1"),("COLUMN2", "TYPE2")),
      "TABLE2" -> List(("COLUMN1", "TYPE3"),("COLUMN2", "TYPE4"))
    )
    // When
    val result = CSVSchemaReader.readColumnsType(inputLines)

    // Then
    assert(result == expected)

  }
}
