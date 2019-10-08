// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.statistics.exploratory

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset

object NumberOfEvents {

  val logger: Logger = Logger.getLogger(getClass)

  def evaluate(
      dcirCompact: Dataset[DcirCompact],
      mcoCompact: Dataset[McoCompact],
      outputPathRoot: String): Unit = {

    computeAndSaveByMonth(dcirCompact, mcoCompact, outputPathRoot)
    computeAndSaveByPatientAndMonth(dcirCompact, mcoCompact, outputPathRoot)
  }

  def computeAndSaveByMonth(
      dcirCompact: Dataset[DcirCompact],
      mcoCompact: Dataset[McoCompact],
      outputPathRoot: String): Unit = {

    val dcirPurchaseCountByMonth = dcirCompact
      .groupBy("Purchase_Date_trunc")
      .count
      .withColumnRenamed("count", "dcir_count")

    val mcoDiagCountByMonth = mcoCompact
      .groupBy("Diagnosis_Date_trunc")
      .count
      .withColumnRenamed("count", "mco_count")

    val dcirOutputPath = outputPathRoot + "/dcirPurchaseCountByMonth"
    val mcoOutputPath = outputPathRoot + "/mcoDiagCountByMonth"

    logger.info(s"Saving number of lines in DCIR by patients under: $dcirOutputPath")
    dcirPurchaseCountByMonth.write.parquet(dcirOutputPath)

    logger.info(s"Saving number of lines in MCO by patients under: $mcoOutputPath")
    mcoDiagCountByMonth.write.parquet(mcoOutputPath)
  }

  def computeAndSaveByPatientAndMonth(
      dcirCompact: Dataset[DcirCompact],
      mcoCompact: Dataset[McoCompact],
      outputPathRoot: String): Unit = {

    val outputPath = outputPathRoot + "/nbDrugPurchaseVsDiagByMonthByPatients"

    val dcirPurchaseCountByPatientAndMonth = dcirCompact
      .groupBy("DCIR_NUM_ENQ", "Purchase_Date_trunc")
      .count
      .withColumnRenamed("count", "dcir_count")

    val mcoMonthCountByPatientAndMonth = mcoCompact
      .groupBy("MCO_NUM_ENQ", "Diagnosis_Date_trunc")
      .count
      .withColumnRenamed("count", "mco_count")

    val dcirOutputPath = outputPathRoot + "/dcirPurchaseCountByPatientAndMonth"
    val mcoOutputPath = outputPathRoot + "/mcoDiagCountByPatientAndMonth"

    logger.info(s"Saving number of lines in DCIR by patients under: $dcirOutputPath")
    dcirPurchaseCountByPatientAndMonth.write.parquet(dcirOutputPath)

    logger.info(s"Saving number of lines in MCO by patients under: $mcoOutputPath")
    mcoMonthCountByPatientAndMonth.write.parquet(mcoOutputPath)
  }
}
