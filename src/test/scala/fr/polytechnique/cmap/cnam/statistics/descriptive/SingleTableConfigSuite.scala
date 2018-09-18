package fr.polytechnique.cmap.cnam.statistics.descriptive

import com.typesafe.config.ConfigFactory
import pureconfig._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.config.ConfigLoader

class SingleTableConfigSuite extends SharedContext with ConfigLoader {

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

  "fromConfig" should "create a SingleTableConfig from a PureConfig instance" in {

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
    val result = loadConfig[SingleTableConfig](config).right.get

    // Then
    assert(result == expected)
  }
}
