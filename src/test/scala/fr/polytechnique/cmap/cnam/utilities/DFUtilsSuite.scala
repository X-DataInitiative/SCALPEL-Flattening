package fr.polytechnique.cmap.cnam.utilities

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import org.apache.spark.sql.types._

/**
  * Created by sathiya on 07/03/17.
  */
class DFUtilsSuite extends SharedContext {
  "applySchema" should "should cast a DF with correct schema" in {
    val sqlCtx = sqlContext
    import  sqlCtx.implicits._

    //Given
    val nullable = true

    val inputDF = Seq(
      ("john", "1", "01/01/2006"),
      ("george", "2", "25/01/2006"),
      ("john", null, null)
    ).toDF("NUM_ENQ", "BEN_NAI_ANN", "BEN_DTE_MAJ")

    val inputSchema = Map(
      "ORG_AFF_BEN" -> "String",
      "BEN_NAI_ANN" -> "String",
      "BEN_DTE_MAJ" -> "Date",
      "NUM_ENQ" -> "String"
    )

    val inputFormat = "dd/MM/yyyy"

    val inputName = "TEST_TABLE"

    val expectedResult = StructType(Seq(
      StructField("NUM_ENQ", StringType, nullable),
      StructField("BEN_NAI_ANN", StringType, nullable),
      StructField("BEN_DTE_MAJ", DateType, nullable)
    ))

    //When
    val result = applySchema(inputDF, inputSchema, inputFormat)

    //Then
    assert(result.schema == expectedResult)
    assert(inputDF.schema != result.schema)
  }


  it should "raise en error when a column is missing" in {
    val sqlCtx = sqlContext
    import  sqlCtx.implicits._

    //Given

    val inputDF = Seq(
      ("john", "1", "01/01/2006"),
      ("george", "2", "25/01/2006"),
      ("john", null, null)
    ).toDF("NUM_ENQ", "BEN_NAI_ANN", "BEN_DTE_MAJ")

    val inputSchema = Map(
      "ORG_AFF_BEN" -> "String",
      "BEN_DTE_MAJ" -> "Date",
      "NUM_ENQ" -> "String"
    )

    val inputFormat = "dd/MM/yyyy"

    val inputName = "TEST_TABLE"


    //When
    //Then
    intercept[java.util.NoSuchElementException] {
      applySchema(inputDF, inputSchema, inputFormat)
    }
  }

  "sameAs" should "return true" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val df1 = sc.parallelize(Seq(1,2,3)).toDF("toto")
    val df2 = sc.parallelize(Seq(1,3,2)).toDF("toto")

    // When
    val result = df1 sameAs df2

    // Then
    assert(result)
  }

  it should "return false" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val df1 = sc.parallelize(Seq(1,2,4)).toDF("toto")
    val df2 = sc.parallelize(Seq(1,3,2)).toDF("toto")

    // When
    val result = df1 sameAs df2

    // Then
    assert(!result)
  }

  it should "return false when inconsistent duplicates are found" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1,2,3,2)).toDF("toto")
    val df2 = sc.parallelize(Seq(1,3,2,3)).toDF("toto")

    // When
    val result = df1 sameAs df2

    // Then
    assert(!result)
  }
}
