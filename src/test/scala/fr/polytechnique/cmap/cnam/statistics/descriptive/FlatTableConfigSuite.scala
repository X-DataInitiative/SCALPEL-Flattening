package fr.polytechnique.cmap.cnam.statistics.descriptive

import com.typesafe.config.ConfigFactory
import pureconfig._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.config.ConfigLoader

class FlatTableConfigSuite extends SharedContext with ConfigLoader {

  val tableName = "A_TABLE"
  val centralTable = "CentralTable"
  val joinKeys = List("COL")
  val dateFormat = "yyyy-MM-dd"
  val inputPath = "/path/to/input/"
  val outputStatPath = "/path/to/output/"
  val singleTables = List(
    SingleTableConfig("A_SINGLE_TABLE", "/path/to/input/single_table")
  )

  "toString" should "print the class members" in {

    // Given
    val expected = {
      s"tableName -> $tableName \n" +
        s"centralTable -> $centralTable \n" +
        s"joinKeys -> $joinKeys \n" +
        s"dateFormat -> $dateFormat \n" +
        s"inputPath -> $inputPath \n" +
        s"outputStatPath -> $outputStatPath \n" +
        s"singleTableCount -> ${singleTables.size}"
    }
    val input = FlatTableConfig(
      tableName,
      centralTable,
      joinKeys,
      dateFormat,
      inputPath,
      outputStatPath,
      singleTables
    )

    // When
    val result = input.toString

    // Then
    assert(result == expected)
  }

  "fromConfig" should "create a FlatTableConfig from a PureConfig instance" in {

    // Given
    val config = ConfigFactory.parseString(
      s"""
        {
          name = $tableName
          central_table = $centralTable
          date_format = $dateFormat
          input_path = $inputPath
          output_stat_path = $outputStatPath
        }
      """.stripMargin)

    val expected = FlatTableConfig(
      tableName,
      centralTable,
      List(),
      dateFormat,
      inputPath,
      outputStatPath,
      List()
    )

    // When
    val result = loadConfig[FlatTableConfig](config).right.get
    // Then
    assert(result == expected)
  }
}
