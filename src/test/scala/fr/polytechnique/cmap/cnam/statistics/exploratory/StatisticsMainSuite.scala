// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.statistics.exploratory

import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils

class StatisticsMainSuite extends SharedContext {

  "run" should "read input DF from given inputPathRoot and save all output under given outputPathRoot" in {

    // Given
    val sqlCtx: SQLContext = sqlContext
    import sqlCtx.implicits._

    val inputPathRoot = "src/test/resources/statistics/descriptive"
    val outputPathRoot = "target/test/output/statistics/descriptive"
    val argsMap = Map("inputPathRoot" -> inputPathRoot, "outputPathRoot" -> outputPathRoot)

    val expectedNbLinesDcirCountByPatient = Seq(
      ("Patient_01", 3L),
      ("Patient_02", 4L)
    ).toDF("DCIR_NUM_ENQ", "dcir_count")

    val expectedNbLinesMcoCountByPatient = Seq(
      ("Patient_02", 6L)
    ).toDF("MCO_NUM_ENQ", "mco_count")

    val expectedNbLinesDcirCountByPatientAndMonth= Seq(
      ("Patient_01", "1-2006", 2L),
      ("Patient_02", "1-2006", 4L),
      ("Patient_01", null, 1L)
    ).toDF("DCIR_NUM_ENQ", "Purchase_Date_trunc", "dcir_count")

    val expectedNbLinesMcoCountByPatientAndMonth = Seq(
      ("Patient_02", "3-2008", 2L),
      ("Patient_02", "2-2007", 2L),
      ("Patient_02", "1-2006", 2L)
    ).toDF("MCO_NUM_ENQ", "Diagnosis_Date_trunc", "mco_count")

    val expectedDcirPurchaseCountByPatient = Seq(
      ("1-2006", 2L),
      (null, 1L)
    ).toDF("Purchase_Date_trunc", "dcir_count")

    val expectedMcoDiagCountByPatient = Seq(
      ("3-2008", 2L),
      ("2-2007", 1L),
      ("1-2006", 1L)
    ).toDF("Diagnosis_Date_trunc", "mco_count")

    val expectedDcirPurchaseCountByPatientAndMonth = Seq(
      ("Patient_01", "1-2006", 1L),
      ("Patient_02", "1-2006", 1L),
      ("Patient_01", null, 1L)
    ).toDF("DCIR_NUM_ENQ", "Purchase_Date_trunc", "dcir_count")

    val expectedMcoDiagCountByPatientAndMonth = Seq(
      ("Patient_02", "3-2008", 2L), // MCO08B_ENT_DAT has two different values for "3-2008"
      ("Patient_02", "2-2007", 1L),
      ("Patient_02", "1-2006", 1L)
    ).toDF("MCO_NUM_ENQ", "Diagnosis_Date_trunc", "mco_count")

    // When
    StatisticsMain.run(sqlCtx, argsMap)

    import DFUtils.readParquet
    val nbLinesDcirCountByPatient = readParquet(sqlCtx, outputPathRoot + "/dcirCountByPatient")
    val nbLinesMcoCountByPatient = readParquet(sqlCtx, outputPathRoot + "/mcoCountByPatient")
    val nbLinesDcirCountByPatientAndMonth = readParquet(sqlCtx, outputPathRoot + "/dcirCountByPatientAndMonth")
    val nbLinesMcoCountByPatientAndMonth = readParquet(sqlCtx, outputPathRoot + "/mcoCountByPatientAndMonth")

    val dcirPurchaseCountByPatient = readParquet(sqlCtx, outputPathRoot + "/dcirPurchaseCountByMonth")
    val mcoDiagCountByPatient = readParquet(sqlCtx, outputPathRoot + "/mcoDiagCountByMonth")
    val dcirPurchaseCountByPatientAndMonth = readParquet(sqlCtx, outputPathRoot + "/dcirPurchaseCountByPatientAndMonth")
    val mcoDiagCountByPatientAndMonth = readParquet(sqlCtx, outputPathRoot + "/mcoDiagCountByPatientAndMonth")

    // Then
    assertDFs(nbLinesDcirCountByPatient, expectedNbLinesDcirCountByPatient)
    assertDFs(nbLinesMcoCountByPatient, expectedNbLinesMcoCountByPatient)
    assertDFs(nbLinesDcirCountByPatientAndMonth, expectedNbLinesDcirCountByPatientAndMonth)
    assertDFs(nbLinesMcoCountByPatientAndMonth, expectedNbLinesMcoCountByPatientAndMonth)

    assertDFs(dcirPurchaseCountByPatient, expectedDcirPurchaseCountByPatient)
    assertDFs(mcoDiagCountByPatient, expectedMcoDiagCountByPatient)
    assertDFs(dcirPurchaseCountByPatientAndMonth, expectedDcirPurchaseCountByPatientAndMonth)
    assertDFs(mcoDiagCountByPatientAndMonth, expectedMcoDiagCountByPatientAndMonth)
  }

}
