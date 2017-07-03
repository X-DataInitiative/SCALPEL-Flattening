package fr.polytechnique.cmap.cnam.statistics.descriptive

import java.sql.Date
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.utilities.DFUtils

case class DcirCompact(
    DCIR_NUM_ENQ: String,
    BEN_NAI_ANN: String,
    BEN_DCD_DTE: Option[Date],
    Purchase_Date_trunc: String)

case class McoCompact(MCO_NUM_ENQ: String, ENT_DAT: Date, Diagnosis_Date_trunc: String)

case class IrBenCompact(NUM_ENQ: String, BEN_NAI_ANN: String, BEN_DCD_DTE: Option[Date])

case class IrImbCompact(NUM_ENQ: String, IMB_ALD_DTD: Date)

object DcirCompact {
  def columns: Seq[Column] = Seq("DCIR_NUM_ENQ", "BEN_NAI_ANN", "BEN_DCD_DTE", "Purchase_Date_trunc").map(col(_))
}

object McoCompact {
  def columns: Seq[Column] = Seq("MCO_NUM_ENQ", "ENT_DAT", "Diagnosis_Date_trunc").map(col(_))
}

object IrBenCompact {
  def columns: Seq[Column] = Seq("NUM_ENQ", "BEN_NAI_ANN", "BEN_DCD_DTE").map(col(_))
}

object IrImbCompact {
  def columns: Seq[Column] = Seq("NUM_ENQ", "IMB_ALD_DTD").map(col(_))
}

object Sources {

  val dcirTruncPurchaseDateCol: Column = concat(
    month(col("EXE_SOI_DTD")),
    lit("-"),
    year(col("EXE_SOI_DTD"))
  )

  val mcoTruncDiagDateCol: Column = concat(
    col("MCO_B__SOR_MOI"),
    lit("-"),
    col("MCO_B__SOR_ANN")
  )

  import DFUtils.readParquet

  def annotatedDcir(sqlContext: SQLContext, dcirFlatPath: String): DataFrame = {
    readParquet(sqlContext, dcirFlatPath)
        .withColumn("Purchase_Date_trunc", dcirTruncPurchaseDateCol)
        .withColumnRenamed("NUM_ENQ", "DCIR_NUM_ENQ")
  }

  def annotatedMco(sqlContext: SQLContext, mcoFlatPath: String): DataFrame = {
    readParquet(sqlContext, mcoFlatPath)
        .withColumn("Diagnosis_Date_trunc", mcoTruncDiagDateCol)
        .withColumnRenamed("NUM_ENQ", "MCO_NUM_ENQ")
  }

  def dcirCompact(annotatedDcir: DataFrame): Dataset[DcirCompact] = {
    import annotatedDcir.sqlContext.implicits._
    annotatedDcir
        .select(DcirCompact.columns: _*)
        .as[DcirCompact]
        .distinct
  }

  def mcoCompact(annotatedMco: DataFrame): Dataset[McoCompact] = {
    import annotatedMco.sqlContext.implicits._
    annotatedMco
        .select(McoCompact.columns: _*)
        .as[McoCompact]
        .distinct
  }

  def irBenCompact(sqlContext: SQLContext, irBenPath: String): Dataset[IrBenCompact] = {
    import sqlContext.implicits._
    readParquet(sqlContext, irBenPath)
        .select(IrBenCompact.columns: _*)
        .as[IrBenCompact]
        .distinct
  }

  def irImbCompact(sqlContext: SQLContext, irImbPath: String): Dataset[IrImbCompact] = {
    import sqlContext.implicits._
    readParquet(sqlContext, irImbPath)
        .select(IrImbCompact.columns: _*)
        .as[IrImbCompact]
        .distinct
  }

}
