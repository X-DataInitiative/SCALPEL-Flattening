// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.statistics.exploratory

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class NumberOfEventsSuite extends SharedContext {

  lazy val sqlCtx: SQLContext = sqlContext

  "computeAndSaveByMonth" should "return corrent output" in {

    // Given
    import utilities.Functions._
    import sqlCtx.implicits._

    val dcirCompact: Dataset[DcirCompact] = Seq(
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "9-1998"),
      DcirCompact("Patient2", "1980", None, "9-1998"),
      DcirCompact("Patient2", "1980", None, "3-1998")
    ).toDS

    val mcoCompact: Dataset[McoCompact] = Seq(
      McoCompact("Patient1", makeTS(1999, 11, 1), "11-1999"),
      McoCompact("Patient1", makeTS(1999, 12, 1), "12-1999"),
      McoCompact("Patient2", makeTS(1999, 10, 1), "11-1999"),
      McoCompact("Patient3", makeTS(1999, 9, 1), "9-1999")
    ).toDS

    val expectedDcirOutput = Seq(
      ("9-1998", 2L),
      ("8-1998", 2L),
      ("3-1998", 1L)
    ).toDF("Purchase_Date_trunc", "dcir_count")

    val expectedMcoOutput = Seq(
      ("11-1999", 2L),
      ("9-1999", 1L),
      ("12-1999", 1L)
    ).toDF("Diagnosis_Date_trunc", "mco_count")

    val outputPathRoot = "target/test/output/statistics/distribution"

    // When
    NumberOfEvents.computeAndSaveByMonth(dcirCompact, mcoCompact, outputPathRoot)

    import utilities.DFUtils.readParquet
    val dcirOutput = readParquet(sqlCtx, outputPathRoot + "/dcirPurchaseCountByMonth")
    val mcoOutput = readParquet(sqlCtx, outputPathRoot + "/mcoDiagCountByMonth")

    // Then
    assertDFs(dcirOutput, expectedDcirOutput, true)
    assertDFs(mcoOutput, expectedMcoOutput, true)
  }

  "computeAndSaveByPatientAndMonth" should "return corrent output" in {

    // Given
    import utilities.Functions._
    import sqlCtx.implicits._

    val dcirCompactInput: Dataset[DcirCompact] = Seq(
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "9-1998"),
      DcirCompact("Patient2", "1980", None, "9-1998"),
      DcirCompact("Patient2", "1980", None, "3-1998")
    ).toDS

    val mcoCompactInput: Dataset[McoCompact] = Seq(
      McoCompact("Patient1", makeTS(1999, 11, 1), "11-1999"),
      McoCompact("Patient1", makeTS(1999, 12, 1), "12-1999"),
      McoCompact("Patient2", makeTS(1999, 10, 1), "11-1999"),
      McoCompact("Patient3", makeTS(1999, 9, 1), "9-1999")
    ).toDS

    val expectedDcirOutput = Seq(
      ("Patient1", "9-1998", 1L),
      ("Patient2", "9-1998", 1L),
      ("Patient1", "8-1998", 2L),
      ("Patient2", "3-1998", 1L)
    ).toDF("DCIR_NUM_ENQ", "Purchase_Date_trunc", "dcir_count")

    val expectedMcoOutput = Seq(
      ("Patient1", "11-1999", 1L),
      ("Patient2", "11-1999", 1L),
      ("Patient3", "9-1999", 1L),
      ("Patient1", "12-1999", 1L)
    ).toDF("MCO_NUM_ENQ", "Diagnosis_Date_trunc", "mco_count")

    val outputPathRoot = "target/test/output/statistics/distribution"

    // When
    NumberOfEvents.computeAndSaveByPatientAndMonth(dcirCompactInput, mcoCompactInput, outputPathRoot)

    import utilities.DFUtils.readParquet
    val dcirOutput = readParquet(sqlCtx, outputPathRoot + "/dcirPurchaseCountByPatientAndMonth")
    val mcoOutput = readParquet(sqlCtx, outputPathRoot + "/mcoDiagCountByPatientAndMonth")

    // Then
    assertDFs(dcirOutput, expectedDcirOutput, true)
    assertDFs(mcoOutput, expectedMcoOutput, true)
  }

  "evaluate" should "return corrent output" in {

    // Given
    import utilities.Functions._
    import sqlCtx.implicits._

    val dcirCompact: Dataset[DcirCompact] = Seq(
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "9-1998"),
      DcirCompact("Patient2", "1980", None, "9-1998"),
      DcirCompact("Patient2", "1980", None, "3-1998")
    ).toDS

    val mcoCompact: Dataset[McoCompact] = Seq(
      McoCompact("Patient1", makeTS(1999, 11, 1), "11-1999"),
      McoCompact("Patient1", makeTS(1999, 12, 1), "12-1999"),
      McoCompact("Patient2", makeTS(1999, 10, 1), "11-1999"),
      McoCompact("Patient3", makeTS(1999, 9, 1), "9-1999")
    ).toDS

    val expectedDcirOutputByPatient = Seq(
      ("9-1998", 2L),
      ("8-1998", 2L),
      ("3-1998", 1L)
    ).toDF("Purchase_Date_trunc", "dcir_count")

    val expectedMcoOutputByPatient = Seq(
      ("11-1999", 2L),
      ("9-1999", 1L),
      ("12-1999", 1L)
    ).toDF("Diagnosis_Date_trunc", "mco_count")

    val expectedDcirOutputByPatientAndMonth = Seq(
      ("Patient1", "9-1998", 1L),
      ("Patient2", "9-1998", 1L),
      ("Patient1", "8-1998", 2L),
      ("Patient2", "3-1998", 1L)
    ).toDF("DCIR_NUM_ENQ", "Purchase_Date_trunc", "dcir_count")

    val expectedMcoOutputByPatientAndMonth = Seq(
      ("Patient1", "11-1999", 1L),
      ("Patient2", "11-1999", 1L),
      ("Patient3", "9-1999", 1L),
      ("Patient1", "12-1999", 1L)
    ).toDF("MCO_NUM_ENQ", "Diagnosis_Date_trunc", "mco_count")

    val outputPathRoot = "target/test/output/statistics/distribution"

    // When
    NumberOfEvents.evaluate(dcirCompact, mcoCompact, outputPathRoot)

    import utilities.DFUtils.readParquet
    val dcirOutputByPatient = readParquet(sqlCtx, outputPathRoot + "/dcirPurchaseCountByMonth")
    val mcoOutputByPatient = readParquet(sqlCtx, outputPathRoot + "/mcoDiagCountByMonth")

    val dcirOutputByPatientAndMonth = readParquet(sqlCtx, outputPathRoot + "/dcirPurchaseCountByPatientAndMonth")
    val mcoOutputByPatientAndMonth = readParquet(sqlCtx, outputPathRoot + "/mcoDiagCountByPatientAndMonth")

    // Then
    assertDFs(dcirOutputByPatient, expectedDcirOutputByPatient)
    assertDFs(mcoOutputByPatient, expectedMcoOutputByPatient)

    assertDFs(dcirOutputByPatientAndMonth, expectedDcirOutputByPatientAndMonth)
    assertDFs(mcoOutputByPatientAndMonth, expectedMcoOutputByPatientAndMonth)
  }
}
