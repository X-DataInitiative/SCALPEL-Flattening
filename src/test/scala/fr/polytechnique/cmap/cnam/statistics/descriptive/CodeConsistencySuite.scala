package fr.polytechnique.cmap.cnam.statistics.descriptive

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class CodeConsistencySuite extends SharedContext {

  lazy val sqlCtx: SQLContext = sqlContext

  "evaluate" should "return right output" in {

    // Given
    import utilities.Functions._
    import sqlCtx.implicits._

    val dcirCompact: Dataset[DcirCompact] = Seq(
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "8-1998"),
      DcirCompact("Patient1", "1981", Some(makeTS(1999, 10, 1)), "9-1998"),
      DcirCompact("Patient2", "1980", None, "9-1998"),
      DcirCompact("Patient2", "1980", None, "3-1998"),
      DcirCompact("Patient3", "1981", None, "3-1998")
    ).toDS

    val irBenCompact: Dataset[IrBenCompact] = Seq(
      IrBenCompact("Patient1", "1981", Some(makeTS(1999, 10, 2))),
      IrBenCompact("Patient3", "1980", None)
    ).toDS

    val mcoCompact: Dataset[McoCompact] = Seq(
      McoCompact("Patient1", makeTS(1999, 11, 1), "11-1999"),
      McoCompact("Patient1", makeTS(1999, 12, 1), "12-1999"),
      McoCompact("Patient2", makeTS(1999, 11, 1), "11-1999"),
      McoCompact("Patient3", makeTS(1999, 9, 1), "9-1999"),
      McoCompact("Patient3", makeTS(1999, 10, 1), "10-1999"),
      McoCompact("Patient4", makeTS(1999, 9, 1), "11-1999")
    ).toDS

    val irImbCompact: Dataset[IrImbCompact] = Seq(
      IrImbCompact("Patient1", makeTS(1999, 11, 1)),
      IrImbCompact("Patient3", makeTS(1999, 8, 1)),
      IrImbCompact("Patient4", makeTS(1999, 10, 1))
    ).toDS

    val outputPathRoot = "target/test/output/statistics/distribution"

    val expectedResult = Seq(
      OutputMetric("Dcir_Irben_CommonPatientIds", 2L),
      OutputMetric("Dcir_Irben_CommonBirthYears", 1L),
      OutputMetric("Dcir_Irben_CommonDeathYears", 1L),
      OutputMetric("Mco_Irimb_CommonPatientIds", 3L),
      OutputMetric("Mco_Irimb_CommonDiagDates", 1L),
      OutputMetric("Dcir_Mco_CommonPatientIds", 3L)
    ).toDS

    // When
    CodeConsistency.evaluate(dcirCompact, irBenCompact, mcoCompact, irImbCompact, outputPathRoot)

    import utilities.DFUtils.readParquet
    val result = readParquet(sqlCtx, outputPathRoot + "/codeConsistency").as[OutputMetric]

    // Then
    assertDSs(expectedResult, result)

  }

}
