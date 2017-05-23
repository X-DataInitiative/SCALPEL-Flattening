package fr.polytechnique.cmap.cnam.statistics

import com.typesafe.config.ConfigFactory
import fr.polytechnique.cmap.cnam.SharedContext

class SingleTableConfigSuite extends SharedContext {

  val tableName = "A_TABLE"
  val dateFormat = "yyyy-MM-dd"
  val inputPath = "/path/to/parquet/"

  "toString" should "print the class members" in {

    // Given
    val expected = {
      s"tableName -> $tableName \n" +
      s"dateFormat -> $dateFormat \n" +
      s"inputPath -> $inputPath"
    }

    // When
    val result = SingleTableConfig(tableName, dateFormat, inputPath).toString

    // Then
    assert(result == expected)
  }

  "fromConfig" should "create a SingleTableConfig from a com.typesafe.config.Config instance" in {

    // Given
    val config = ConfigFactory.parseString(
      s"""
        {
          table_name = $tableName
          date_format = $dateFormat
          input_path = $inputPath
        }
      """.stripMargin)

    val expected = SingleTableConfig(tableName, dateFormat, inputPath)

    // When
    val result = SingleTableConfig.fromConfig(config)

    // Then
    assert(result == expected)
  }
}
