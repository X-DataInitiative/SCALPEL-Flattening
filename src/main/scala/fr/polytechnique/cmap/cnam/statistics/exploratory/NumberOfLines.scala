package fr.polytechnique.cmap.cnam.statistics.exploratory

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object NumberOfLines {

  val logger: Logger = Logger.getLogger(getClass)

  def evaluate(
      dcir: DataFrame,
      mco: DataFrame,
      outputPathRoot: String): Unit = {

    computeAndSaveByPatient(dcir, mco, outputPathRoot)
    computeAndSaveByPatientAndMonth(dcir, mco, outputPathRoot)
  }

  def computeAndSaveByPatient(
      dcir: DataFrame,
      mco: DataFrame,
      outputPathRoot: String): Unit = {

    val dcirCountByPatient = dcir
      .groupBy("DCIR_NUM_ENQ")
      .count
      .withColumnRenamed("count", "dcir_count")

    val mcoCountByPatient = mco
      .groupBy("MCO_NUM_ENQ")
      .count
      .withColumnRenamed("count", "mco_count")

    val dcirOutputPath = outputPathRoot + "/dcirCountByPatient"
    val mcoOutputPath = outputPathRoot + "/mcoCountByPatient"

    logger.info(s"Saving number of lines in DCIR by patients under: $dcirOutputPath")
    dcirCountByPatient.write.parquet(dcirOutputPath)

    logger.info(s"Saving number of lines in MCO by patients under: $mcoOutputPath")
    mcoCountByPatient.write.parquet(mcoOutputPath)
  }

  def computeAndSaveByPatientAndMonth(
      dcir: DataFrame,
      mco: DataFrame,
      outputPathRoot: String): Unit = {

    val dcirCountByPatientAndMonth = dcir
      .groupBy("DCIR_NUM_ENQ", "Purchase_Date_trunc")
      .count
      .withColumnRenamed("count", "dcir_count")

    val mcoCountByPatientAndMonth = mco
      .groupBy("MCO_NUM_ENQ", "Diagnosis_Date_trunc")
      .count
      .withColumnRenamed("count", "mco_count")

    val dcirOutputPath = outputPathRoot + "/dcirCountByPatientAndMonth"
    val mcoOutputPath = outputPathRoot + "/mcoCountByPatientAndMonth"

    logger.info(s"Saving number of lines in DCIR by patients and Purchase date under: $dcirOutputPath")
    dcirCountByPatientAndMonth.write.parquet(dcirOutputPath)

    logger.info(s"Saving number of lines in MCO by patients and diagnosis date under: $mcoOutputPath")
    mcoCountByPatientAndMonth.write.parquet(mcoOutputPath)
  }
}
