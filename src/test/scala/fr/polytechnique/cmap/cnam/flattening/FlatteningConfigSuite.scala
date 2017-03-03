package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.FlatteningTableConfig

/**
  * Created by sathiya on 21/02/17.
  */
class FlatteningConfigSuite extends SharedContext {

  "toString" should "correctly return the default parameters from the test flattening config file" in {

    // Given
    val expectedResult =
      s"envName -> test \n" +
        s"schemaFile -> List(src/main/resources/flattening/config/schema/DCIR_schema.csv, " +
                           s"src/main/resources/flattening/config/schema/PMSI_schema.csv) \n" +
        s"outputPath -> target/test/output \n" +
        s"tablesConfigList -> List(ER_CAM_F, ER_PHA_F, ER_PRS_F, ER_UCD_F, IR_BEN_R, IR_IMB_R, " +
                                 s"MCO06A, MCO07A, MCO08A, MCO06B, MCO07B, MCO08B, MCO06C, MCO07C, " +
                                 s"MCO08C, MCO06D, MCO07D, MCO08D, MCO08MED, MCO08UM)"

    // When
    val result = FlatteningConfig.toString

    // Then
    println("Result:")
    println(result)
    println("Expected Result:")
    println(expectedResult)

    assert(result == expectedResult)
  }

  "tableConfig" should "correctly retrieves the corresponding table configutation" in {

    // Given
    val tableName = "ER_PRS_F"
    val expectedResult: String =
      s"name -> ER_PRS_F \n" +
        s"schemaName -> ER_PRS_F \n" +
        s"dateFormat -> dd/MM/yyyy \n" +
        s"inputPaths -> List(src/test/resources/flattening/csv-table/DCIR/ER_PRS_F.csv) \n" +
        s"outputTableName -> ER_PRS_F \n" +
        s"partitionColumn -> Some(1)"

    // When
    val tableConfig: FlatteningTableConfig = FlatteningConfig.tableConfig(tableName)
    val result = tableConfig.toString

    // Then
    println("Result :")
    println(result.toString)
    println("Expected Result :")
    println(expectedResult.toString)

    assert(result == expectedResult)
  }

  "tablesConfigOutputPath" should "return the output path of all the tables configuration " +
    "defined under test env" in {

    // Given
    val expectedResult = List("ER_CAM_F", "ER_PHA_F", "ER_PRS_F", "ER_UCD_F", "IR_BEN_R",
      "IR_IMB_R", "MCO_A/2006", "MCO_A/2007", "MCO_A/2008", "MCO_B/2006", "MCO_B/2007",
      "MCO_B/2008", "MCO_C/2006", "MCO_C/2007", "MCO_C/2008", "MCO_D/2006", "MCO_D/2007",
      "MCO_D/2008", "MCO_MED/2008", "MCO_UM/2008")

    // When
    val result = FlatteningConfig.tablesConfigOutputPath

    // Then
    println("Result :")
    println(result.toString)
    println("Expected Result :")
    println(expectedResult)

    assert(result == expectedResult)
  }

  "implicit toString" should "correctly extract the respective table config parameters" in {

    import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.FlatteningTableConfig

    // Given
    val tableConfig: FlatteningTableConfig = FlatteningConfig.tableConfig("IR_IMB_R")
    val expectedResult: String =
      s"name -> IR_IMB_R \n" +
        s"schemaName -> IR_IMB_R \n" +
        s"dateFormat -> dd/MM/yyyy \n" +
        s"inputPaths -> List(src/test/resources/flattening/csv-table/DCIR/IR_IMB_R.csv) \n" +
        s"outputTableName -> IR_IMB_R \n" +
        s"partitionColumn -> Some(1)"

    // When
    val result = tableConfig.toString

    // Then
    println("Result: ")
    println(result)
    println("Expected Result: ")
    println(expectedResult)

    assert(result == expectedResult)
  }
}
