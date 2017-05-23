package fr.polytechnique.cmap.cnam.statistics

import com.typesafe.config.ConfigFactory
import fr.polytechnique.cmap.cnam.SharedContext

class FlatTableConfigSuite extends SharedContext {

  val tableName = "A_TABLE"
  val centralTable = "CentralTable"
  val dateFormat = "yyyy-MM-dd"
  val inputPath = "/path/to/input/"
  val outputStatPath = "/path/to/output/"
  val singleTables = List(
    SingleTableConfig("A_SINGLE_TABLE", "yyyy-MM-dd", "/path/to/input/single_table")
  )

  "toString" should "print the class members" in {

    // Given
    val expected = {
      s"tableName -> $tableName \n" +
      s"centralTable -> $centralTable \n" +
      s"dateFormat -> $dateFormat \n" +
      s"inputPath -> $inputPath \n" +
      s"outputStatPath -> $outputStatPath \n" +
      s"singleTableCount -> ${singleTables.size}"
    }
    val input = FlatTableConfig(
      tableName,
      centralTable,
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

  "fromConfig" should "create a FlatTableConfig from a com.typesafe.config.Config instance" in {

    // Given
    val config = ConfigFactory.parseString(
      s"""
        {
          table_name = $tableName
          main_table = $centralTable
          date_format = $dateFormat
          input_path = $inputPath
          output_stat_path = $outputStatPath
        }
      """.stripMargin)

    val expected = FlatTableConfig(
      tableName,
      centralTable,
      dateFormat,
      inputPath,
      outputStatPath,
      List()
    )

    // When
    val result = FlatTableConfig.fromConfig(config)

    // Then
    assert(result == expected)
  }
}
