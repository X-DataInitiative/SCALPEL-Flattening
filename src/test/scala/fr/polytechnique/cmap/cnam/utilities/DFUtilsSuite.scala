package fr.polytechnique.cmap.cnam.utilities

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.types._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils._

class DFUtilsSuite extends SharedContext {

  "readCSV" should "read CSV files with correct header and delimiters correctly" in {

    //Given
    val sqlCtx = sqlContext
    val inputPath = Seq("src/test/resources/flattening/csv-table/IR_BEN_R.csv",
      "src/test/resources/flattening/csv-table/IR_BEN_R.csv")
    val expectedResult = sqlCtx
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(inputPath: _*)

    //When
    val result = DFUtils.readCSV(sqlCtx, inputPath)

    //Then
    assert(result sameAs expectedResult)
  }

  "readParquet" should "read parquet file merging the schema of the patitions" in {

    //Given
    val sqlCtx = sqlContext
    val inputPath = "src/test/resources/flattening/parquet-table/single_table/MCO_A"
    val expectedResult = sqlCtx
      .read
      .option("mergeSchema", "true")
      .parquet(inputPath)

    //When
    val result = DFUtils.readParquet(sqlCtx, inputPath)

    //Then
    assert(result sameAs expectedResult)
  }


  "applySchema" should "should cast a DF with correct schema" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

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
    import sqlCtx.implicits._

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

    //WhenThen
    intercept[java.util.NoSuchElementException] {
      applySchema(inputDF, inputSchema, inputFormat)
    }
  }

  "sameAs" should "return true" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val df1 = sc.parallelize(Seq(1, 2, 3)).toDF("toto")
    val df2 = sc.parallelize(Seq(1, 3, 2)).toDF("toto")

    // When
    val result = df1 sameAs df2

    // Then
    assert(result)
  }

  it should "return false" in {

    // Given
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    val df1 = sc.parallelize(Seq(1, 2, 4)).toDF("toto")
    val df2 = sc.parallelize(Seq(1, 3, 2)).toDF("toto")

    // When
    val result = df1 sameAs df2

    // Then
    assert(!result)
  }

  it should "return false when inconsistent duplicates are found" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    // Given
    val df1 = sc.parallelize(Seq(1, 2, 3, 2)).toDF("toto")
    val df2 = sc.parallelize(Seq(1, 3, 2, 3)).toDF("toto")

    // When
    val result = df1 sameAs df2

    // Then
    assert(!result)
  }

  "reorder" should "order correctly a dataframe" in {

    //Given
    val sqlctx = sqlContext
    import sqlctx.implicits._

    val df = Seq(("p1", 1, "hiver"),
      ("p2", 2, "hiver"))
      .toDF("patient", "key", "saison")
    val expected = Seq((1, "p1", "hiver"),
      (2, "p2", "hiver"))
      .toDF("key", "patient", "saison")

    //when
    val result = df.reorder

    //Then
    assert(result sameAs expected)

  }

  "writeParquet" should "write the data correctly in a parquet with the different strategies" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    //Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = "target/test/output"
    val expected = data
    val expectedAppend = data.union(data)
    //When
    data.writeParquet(path)("overwrite")
    val result = spark.read.parquet(path)
    val exception = intercept[Exception] {
      data.writeParquet(path)("errorIfExists")
    }
    data.writeParquet(path)("append")
    val resultAppend = spark.read.parquet(path)
    data.writeParquet("target/test/dummy/output")("withTimestamp")
    val resultWithTimestamp = spark.read.parquet("target/test/dummy/output")
    //Then
    // Then
    assertDFs(result, expected)
    assert(exception.isInstanceOf[AnalysisException])
    assertDFs(resultAppend, expectedAppend)
    assertDFs(resultWithTimestamp, expected)
  }

  "writeCSV" should "write the data correctly in a CSV with the different strategies" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._
    //Given
    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = "target/test/output.csv"
    val expected = data
    val expectedAppend = data.union(data)
    //When
    data.writeCSV(path)("overwrite")
    val result = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
    val exception = intercept[Exception] {
      data.writeCSV(path)("errorIfExists")
    }
    data.writeCSV(path)("append")
    val resultAppend = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
    data.writeCSV("target/test/dummy/output.csv")("withTimestamp")
    val resultWithTimestamp = spark.read
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("target/test/dummy/output.csv")
    // Then
    assertDFs(result, expected)
    assert(exception.isInstanceOf[AnalysisException])
    assertDFs(resultAppend, expectedAppend)
    assertDFs(resultWithTimestamp, expected)
  }

  "withTimestampSuffix" should "add a timestamp at the end of the path" in {
    //Given
    val path = "/first/second/third/"
    val format = new SimpleDateFormat("/yyyy_MM_dd")
    val now = new Date()
    val expected = s"/first/second/third${format.format(now)}"
    //When
    val result = path.withTimestampSuffix(now).toString
    //Then
    assert(result == expected)
  }


}
