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

    val commonPatientIdDcirIrben = 2L
    val commonBirthYearDcirIrben = 1L
    val commonDeathYearDcirIrben = 1L

    val commonPatientIdMcoIrimb = 3L
    val commonDiagDateMcoIrimb = 1L

    val commonPatientIdDcirMco = 3L

    val expectedResult = Seq(commonPatientIdDcirIrben, commonBirthYearDcirIrben,
      commonDeathYearDcirIrben, commonPatientIdMcoIrimb, commonDiagDateMcoIrimb,
      commonPatientIdDcirMco)

    // When
    val result = CodeConsistency.evaluate(dcirCompact, irBenCompact, mcoCompact, irImbCompact)

    // Then
    assert(expectedResult === result)
  }

}
