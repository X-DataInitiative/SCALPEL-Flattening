package fr.polytechnique.cmap.cnam.utilities.reporting

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.Path
import org.apache.spark.sql.{AnalysisException, SQLContext}

class OperationReporterSuite extends SharedContext {

  lazy val sqlCtx: SQLContext = super.sqlContext

  "report" should "Return the correct metadata" in {
    import sqlCtx.implicits._

    // Given

    val data = Seq(("Patient_A", 1), ("Patient_A", 2), ("Patient_B", 3)).toDF("patientID", "other_col")
    val path = Path("target/test/output")

    val expected = OperationMetadata(
      List("mcoce","mco"),
      List("ETA_NUM"),
      List("ddMMMyyyy"),
      List("/test/input/mco","/test/input/mcoce"),
      List("dcir"),
      path.toString
    )

    // When
    val result: OperationMetadata = OperationReporter.report(List("mcoce","mco"),  List("ETA_NUM"),
      List("ddMMMyyyy"), List("/test/input/mco","/test/input/mcoce"), List("dcir"), path/*, data*/)

    // Then
    assert(result == expected)
  }

  it should "Write the data correctly" in {
    import sqlCtx.implicits._

    // Given
    val data = Seq(("Patient_A", 1), ("Patient_B", 2), ("Patient_C", 3)).toDF("patientID", "other_col")
    val databis = Seq(("Patient_A", "kev"), ("Patient_B", "jean"), ("Patient_C", "nicolas")).toDF("patientID", "name")
    val path = Path("target/test/output")
    val expected = data
    val expectedAppend = data.union(databis)

    // When
    var meta = OperationReporter.report(List("dcir","mco"),List("ETA_NUM"),
      List("ddMMMyyyy"), List("/target/input"), List("dcir"), path, /*data.toDF,*/ "overwrite")
    val result = spark.read.parquet(meta.outputPath)
    val exception = intercept[Exception] {
      OperationReporter.report(List("dcir","mco"),List("ETA_NUM"),
        List("ddMMMyyyy"), List("/target/input"), List("dcir"), path/*, data.toDF*/)
    }
    meta = OperationReporter.report(List("dcir","mco"),List("ETA_NUM"),
      List("ddMMMyyyy"), List("/target/input"), List("dcir"), path, /*data.toDF,*/  "append")
    val resultAppend = spark.read.parquet(meta.outputPath)


    // Then
    assertDFs(result, expected)
    assert(exception.isInstanceOf[AnalysisException])
    assertDFs(resultAppend, expectedAppend)
  }
}
