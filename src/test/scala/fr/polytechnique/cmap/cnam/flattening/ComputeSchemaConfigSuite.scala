package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 03/03/17.
  */
class ComputeSchemaConfigSuite extends SharedContext {

  "toString" should "correctly return the default parameters from the computeSchema config file" in {

    // Given
    val expectedResult =
      s"envName -> default \n" +
        s"outputPath -> target/test/output/computedSchema \n" +
        s"rawSchemaFilePath -> List(src/test/resources/flattening/raw-schema/DCIR_raw_schema.csv, " +
                                  s"src/test/resources/flattening/raw-schema/PMSI_raw_schema.csv) \n" +
        s"databaseTablesMap -> Map(" +
            s"DCIR -> List(IR_BEN_R, IR_IMB_R, ER_PRS_F, ER_PHA_F, ER_UCD_F, ER_CAM_F), " +
            s"PMSI -> List(T_MCO__A, T_MCO__B, T_MCO__C, T_MCO__D, T_MCO__UM, T_MCO__MED, T_MCO__FH))"

    // When
    val result = ComputeSchemaConfig.toString

    // Then
    println("Result:")
    println(result)
    println("Expected Result:")
    println(expectedResult)

    assert(result == expectedResult)
  }
}
