package fr.polytechnique.cmap.cnam.statistics.exploratory

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset

case class OutputMetric(name: String, value: Long)

object CodeConsistency {

  val logger: Logger = Logger.getLogger(getClass)

  def evaluate(
      dcirCompact: Dataset[DcirCompact],
      irbenCompact: Dataset[IrBenCompact],
      mcoCompact: Dataset[McoCompact],
      irimbCompact: Dataset[IrImbCompact],
      outputPathRoot: String): Unit = {

    import dcirCompact.sqlContext.implicits._

    val codeConsistencyMetrics = Seq(
      getDcirIrBenMetrics(dcirCompact, irbenCompact),
      getMcoIrImbMetrics(mcoCompact, irimbCompact),
      getDcirMcoMertic(dcirCompact, mcoCompact)
    ).map(_.toDS).reduce(_ union _)

    codeConsistencyMetrics.write.parquet(outputPathRoot + "/codeConsistency")
  }

  def getDcirIrBenMetrics(
      dcirCompact: Dataset[DcirCompact],
      irBenCompact: Dataset[IrBenCompact]): Seq[OutputMetric] = {

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

    Seq(
      OutputMetric("Dcir_Irben_CommonPatientIds", commonPatientIdsInDcirAndIrben),
      OutputMetric("Dcir_Irben_CommonBirthYears", commonBirthYearsInDcirAndIrben),
      OutputMetric("Dcir_Irben_CommonDeathYears", commonDeathYearsInDcirAndIrben)
    )

  }

  def getMcoIrImbMetrics(
      mcoCompact: Dataset[McoCompact],
      irImbCompact: Dataset[IrImbCompact]): Seq[OutputMetric] = {

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

    val commonDiagDatesInMcoAndIrImb = irImbCompact
      .select("NUM_ENQ", "IMB_ALD_DTD")
      .distinct
      .intersect(
          mcoCompact
            .select("MCO_NUM_ENQ", "ENT_DAT")
            .distinct
      ).count

    logger.info(s"Number of patient id's in common between MCO and IR_IMB_R: $commonPatientIdsInMcoAndIrImb")
    logger.info(s"Number of diagnosis dates in common between MCO and IR_IMB_R: $commonDiagDatesInMcoAndIrImb")

    Seq(
      OutputMetric("Mco_Irimb_CommonPatientIds", commonPatientIdsInMcoAndIrImb),
      OutputMetric("Mco_Irimb_CommonDiagDates", commonDiagDatesInMcoAndIrImb)
    )

  }

  def getDcirMcoMertic(
      dcirCompact: Dataset[DcirCompact],
      mcoCompact: Dataset[McoCompact]): Seq[OutputMetric] = {

    val commonPatientIdsInDcirAndMco = dcirCompact
      .select("DCIR_NUM_ENQ")
      .distinct
      .intersect(
          mcoCompact
            .select("MCO_NUM_ENQ")
            .distinct
      ).count

    logger.info(s"Number of patient id's in common between DCIR and MCO: $commonPatientIdsInDcirAndMco")

    Seq(
      OutputMetric("Dcir_Mco_CommonPatientIds", commonPatientIdsInDcirAndMco)
    )

  }
}
