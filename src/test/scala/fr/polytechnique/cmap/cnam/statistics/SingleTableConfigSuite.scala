package fr.polytechnique.cmap.cnam.statistics

import com.typesafe.config.ConfigFactory
import fr.polytechnique.cmap.cnam.SharedContext

class SingleTableConfigSuite extends SharedContext {

  val tableName = "A_TABLE"
  val inputPath = "/path/to/parquet/"

  "toString" should "print the class members" in {

    // Given
    val expected = {
      s"tableName -> $tableName \n" +
      s"inputPath -> $inputPath"
    }

    // When
    val result = SingleTableConfig(tableName, inputPath).toString

    // Then
    assert(result == expected)
  }

  "fromConfig" should "create a SingleTableConfig from a com.typesafe.config.Config instance" in {

    // Given
    val config = ConfigFactory.parseString(
      s"""
        {
          name = $tableName
          input_path = $inputPath
        }
      """.stripMargin)

    val expected = SingleTableConfig(tableName, inputPath)

    // When
    val result = SingleTableConfig.fromConfig(config)

    // Then
    assert(result == expected)
  }
}
