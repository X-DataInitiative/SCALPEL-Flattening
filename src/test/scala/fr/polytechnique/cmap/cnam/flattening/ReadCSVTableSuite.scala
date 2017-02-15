package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 15/02/17.
  */
class ReadCSVTableSuite extends SharedContext {

  "getSchema" should "compute the right data types of the columns based on the format types " +
    "specified in the Schema file" in {

    //Given
    val schemaFile = sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("src/test/resources/flattening/schema/DCIR_schema.csv")

    val tableName = "IR_BEN_R"
    val expectedResult: Map[String, String] = Map(
      "BEN_CDI_NIR" ->  "String",
      "BEN_DTE_INS" ->  "Date",
      "BEN_DTE_MAJ" ->  "Date",
      "BEN_NAI_ANN" ->  "String",
      "BEN_NAI_MOI" ->  "String",
      "BEN_RES_COM" ->  "String",
      "BEN_RES_DPT" ->  "String",
      "BEN_RNG_GEM" ->  "Integer",
      "BEN_SEX_COD" ->  "Integer",
      "BEN_TOP_CNS" ->  "Integer",
      "MAX_TRT_DTD" ->  "Date",
      "ORG_CLE_NEW" ->  "String",
      "ORG_AFF_BEN" ->  "String",
      "BEN_DCD_AME" ->  "String",
      "BEN_DCD_DTE" ->  "Date",
      "BEN_IDT_TOP" -> "Integer",
      "BEN_NIR_ANO" -> "String",
      "ASS_NIR_ANO" -> "String",
      "BEN_IDT_ANO" -> "String",
      "BEN_NIR_PSA" -> "String")


    //When
    val result = ReadCSVTable.getSchema(schemaFile, tableName)

    //Then
    println("result.diff(expectedResult):")
    println (result.toSet.diff(expectedResult.toSet))
    println("expectedResult.diff(result):")
    println (expectedResult.toSet.diff(result.toSet))

    assert(result == (expectedResult))
  }

  "applySchema" should "should cast the input DF columns to the right data types" in {

    //Given
    val schemaFile = sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("src/test/resources/flattening/schema/DCIR_schema.csv")

    val input = sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("src/test/resources/flattening/csv-table/IR_BEN_R.csv")

    val tableName = "IR_BEN_R"
    val config = FlatteningConfig.getTableConfig(tableName)
    val expectedResult = StructType(Seq(
        StructField("BEN_CDI_NIR", StringType, true),
        StructField("BEN_DTE_INS", DateType, true),
        StructField("BEN_DTE_MAJ", DateType, true),
        StructField("BEN_NAI_ANN", StringType, true),
        StructField("BEN_NAI_MOI", StringType, true),
        StructField("BEN_RES_COM", StringType, true),
        StructField("BEN_RES_DPT", StringType, true),
        StructField("BEN_RNG_GEM", IntegerType, true),
        StructField("BEN_SEX_COD", IntegerType, true),
        StructField("BEN_TOP_CNS", IntegerType, true),
        StructField("MAX_TRT_DTD", DateType, true),
        StructField("ORG_CLE_NEW", StringType, true),
        StructField("ORG_AFF_BEN", StringType, true),
        StructField("BEN_DCD_AME", StringType, true),
        StructField("BEN_DCD_DTE", DateType, true),
        StructField("NUM_ENQ", StringType, true)
    ))

    //When
    import fr.polytechnique.cmap.cnam.flattening.ReadCSVTable.FlatteningDFUtilities
    val result = input.applySchema(schemaFile, config)


    //Then
    println("Result:")
    result.schema.fields foreach println
    println("Expected Result:")
    expectedResult foreach println

    assert(result.schema == expectedResult)
    assert(input.schema != result.schema)
  }

  it should "test correctly" in {
    val input = sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv("src/test/resources/flattening/csv-table/IR_BEN_R.csv")

    input.show
    input.printSchema


  }
}
