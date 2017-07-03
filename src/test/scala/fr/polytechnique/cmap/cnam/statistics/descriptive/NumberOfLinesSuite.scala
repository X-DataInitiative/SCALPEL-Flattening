package fr.polytechnique.cmap.cnam.statistics.descriptive

import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class NumberOfLinesSuite extends SharedContext {

  lazy val sqlCtx: SQLContext = sqlContext

  "evaluate" should "call `computeAndSaveByPatient` and `computeAndSaveByPatientAndMonth` functions " +
      "and save expected output under given outputRoot path" in {

    // Given
    import utilities.Functions._
    import sqlCtx.implicits._

    val dcirCompact: DataFrame = Seq(
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "9-1998"),
      DcirCompact("Patient2", "1980", None, "9-1998"),
      DcirCompact("Patient2", "1980", None, "3-1998"),
      DcirCompact("Patient3", "1981", None, "3-1998")
    ).toDF

    val mcoCompact: DataFrame = Seq(
      McoCompact("Patient1", makeTS(1999, 11, 1), "11-1999"),
      McoCompact("Patient1", makeTS(1999, 12, 1), "12-1999"),
      McoCompact("Patient2", makeTS(1999, 10, 1), "11-1999"),
      McoCompact("Patient3", makeTS(1999, 9, 1), "9-1999"),
      McoCompact("Patient3", makeTS(1999, 9, 1), "10-1999")
    ).toDF

    val expectedDcirOutputByPatient = Seq(
      ("Patient1", 3L),
      ("Patient2", 2L),
      ("Patient3", 1L)
    ).toDF("DCIR_NUM_ENQ", "dcir_count")

    val expectedMcoOutputByPatient = Seq(
      ("Patient1", 2L),
      ("Patient2", 1L),
      ("Patient3", 2L)
    ).toDF("MCO_NUM_ENQ", "mco_count")

    val expectedDcirOutputByPatientAndMonth = Seq(
      ("Patient1", "9-1998", 1L),
      ("Patient2", "9-1998", 1L),
      ("Patient1", "8-1998", 2L),
      ("Patient2", "3-1998", 1L),
      ("Patient3", "3-1998", 1L)
    ).toDF("DCIR_NUM_ENQ", "Purchase_Date_trunc", "dcir_count")

    val expectedMcoOutputByPatientAndMonth = Seq(
      ("Patient1", "11-1999", 1L),
      ("Patient2", "11-1999", 1L),
      ("Patient3", "9-1999", 1L),
      ("Patient1", "12-1999", 1L),
      ("Patient3", "10-1999", 1L)
    ).toDF("MCO_NUM_ENQ", "Diagnosis_Date_trunc", "mco_count")

    val outputPathRoot = "target/test/output/statistics/distribution"

    // When
    import utilities.DFUtils.readParquet

    NumberOfLines.evaluate(dcirCompact, mcoCompact, outputPathRoot)
    val dcirCountByPatient = readParquet(sqlCtx, outputPathRoot + "/dcirCountByPatient")
    val mcoCountByPatient = readParquet(sqlCtx, outputPathRoot + "/mcoCountByPatient")

    val dcirCountByPatientAndMonth = readParquet(sqlCtx, outputPathRoot + "/dcirCountByPatientAndMonth")
    val mcoCountByPatientAndMonth = readParquet(sqlCtx, outputPathRoot + "/mcoCountByPatientAndMonth")

    // Then
    assertDFs(dcirCountByPatient, expectedDcirOutputByPatient)
    assertDFs(mcoCountByPatient, expectedMcoOutputByPatient)

    assertDFs(dcirCountByPatientAndMonth, expectedDcirOutputByPatientAndMonth)
    assertDFs(mcoCountByPatientAndMonth, expectedMcoOutputByPatientAndMonth)
  }
}
