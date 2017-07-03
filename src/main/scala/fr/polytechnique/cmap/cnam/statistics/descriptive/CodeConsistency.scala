package fr.polytechnique.cmap.cnam.statistics.descriptive

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset

object CodeConsistency {

  val logger: Logger = Logger.getLogger(getClass)

  def evaluate(
      dcirCompact: Dataset[DcirCompact],
      irbenCompact: Dataset[IrBenCompact],
      mcoCompact: Dataset[McoCompact],
      irimbCompact: Dataset[IrImbCompact]): Seq[Long] = {

    val dcirIrben = printDcirIrBenConsistency(dcirCompact, irbenCompact)
    val mcoIrimb = printMcoIrImbConsistency(mcoCompact, irimbCompact)
    val dcirMco = printDcirMcoConsistency(dcirCompact, mcoCompact)

    dcirIrben ++ mcoIrimb ++ dcirMco
  }

  def printDcirIrBenConsistency(
      dcirCompact: Dataset[DcirCompact],
      irBenCompact: Dataset[IrBenCompact]): Seq[Long] = {

    val nbUniquePatientsInDcir = dcirCompact.select("DCIR_NUM_ENQ").distinct.count
    val nbUniquePatientsInIrben = irBenCompact.select("NUM_ENQ").distinct.count

    logger.info(s"Number of unique patient id's in DCIR: $nbUniquePatientsInDcir")
    logger.info(s"Number of unique patient id's in IR_BEN_R: $nbUniquePatientsInIrben")

    val commonPatientIdsInDcirAndIrben = dcirCompact
        .select("DCIR_NUM_ENQ")
        .distinct
        .intersect(
          irBenCompact
              .select("NUM_ENQ")
              .distinct
        ).count

    val commonBirthYearsInDcirAndIrben = irBenCompact
        .select("NUM_ENQ", "BEN_NAI_ANN")
        .distinct
        .intersect(
          dcirCompact
              .select("DCIR_NUM_ENQ", "BEN_NAI_ANN")
              .distinct
        ).count

    val commonDeathYearsInDcirAndIrben = irBenCompact
        .select("NUM_ENQ", "BEN_DCD_DTE")
        .distinct
        .intersect(
          dcirCompact
              .select("DCIR_NUM_ENQ", "BEN_DCD_DTE")
              .distinct
        ).count

    logger.info(s"Number of patient id's in common between DCIR and IR_BEN_R: $commonPatientIdsInDcirAndIrben")
    logger.info(s"Number of date of birth in common between DCIR and IR_BEN_R: $commonBirthYearsInDcirAndIrben")
    logger.info(s"Number of death dates in common between DCIR and IR_BEN_R: $commonDeathYearsInDcirAndIrben")

    Seq(commonPatientIdsInDcirAndIrben, commonBirthYearsInDcirAndIrben, commonDeathYearsInDcirAndIrben)
  }

  def printMcoIrImbConsistency(
      mcoCompact: Dataset[McoCompact],
      irImbCompact: Dataset[IrImbCompact]): Seq[Long] = {

    val nbUniquePatientsInMco = mcoCompact.select("MCO_NUM_ENQ").distinct.count
    val nbUniquePatientsInIrImb = irImbCompact.select("NUM_ENQ").distinct.count

    logger.info(s"Number of unique patient id's in DCIR: $nbUniquePatientsInMco")
    logger.info(s"Number of unique patient id's in IR_BEN_R: $nbUniquePatientsInIrImb")

    val commonPatientIdsInMcoAndIrImb = irImbCompact
        .select("NUM_ENQ")
        .distinct
        .intersect(
          mcoCompact
              .select("MCO_NUM_ENQ")
              .distinct
        ).count

    val commonDiagDateInMcoAndIrImb = irImbCompact
        .select("NUM_ENQ", "IMB_ALD_DTD")
        .distinct
        .intersect(
          mcoCompact
              .select("MCO_NUM_ENQ", "ENT_DAT")
              .distinct
        ).count

    logger.info(s"Number of patient id's in common between MCO and IR_IMB_R: $commonPatientIdsInMcoAndIrImb")
    logger.info(s"Number of diagnosis dates in common between MCO and IR_IMB_R: $commonDiagDateInMcoAndIrImb")
    Seq(commonPatientIdsInMcoAndIrImb, commonDiagDateInMcoAndIrImb)
  }

  def printDcirMcoConsistency(
      dcirCompact: Dataset[DcirCompact],
      mcoCompact: Dataset[McoCompact]): Seq[Long] = {

    val commonPatientIdsInDcirAndMco = dcirCompact
        .select("DCIR_NUM_ENQ")
        .distinct
        .intersect(
          mcoCompact
              .select("MCO_NUM_ENQ")
              .distinct
        ).count

    logger.info(s"Number of patient id's in common between DCIR and MCO: $commonPatientIdsInDcirAndMco")
    Seq(commonPatientIdsInDcirAndMco)
  }
}
