package fr.polytechnique.cmap.cnam.statistics.exploratory

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main

object StatisticsMain extends Main {

  override def appName = "DistributionStat"

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    val inputPathRoot = argsMap.get("inputPathRoot").get
    val outputPathRoot = argsMap.get("outputPathRoot").get

    val dcirFlatPath = inputPathRoot + "/flat_table/DCIR"
    val mcoFlatPath = inputPathRoot + "/flat_table/MCO"
    val irBenPath = inputPathRoot + "/single_table/IR_BEN_R"
    val irImbPath = inputPathRoot + "/single_table/IR_IMB_R"

    val dcir = Sources.annotatedDcir(sqlContext, dcirFlatPath)

    val mco  = Sources.annotatedMco(sqlContext, mcoFlatPath)

    val dcirCompact = Sources.dcirCompact(dcir).persist

    val mcoCompact = Sources.mcoCompact(mco).persist

    val irBenCompact = Sources.irBenCompact(sqlContext, irBenPath).persist

    val irImbCompact = Sources.irImbCompact(sqlContext, irImbPath).persist

    NumberOfLines.evaluate(dcir, mco, outputPathRoot)
    NumberOfEvents.evaluate(dcirCompact, mcoCompact, outputPathRoot)
    CodeConsistency.evaluate(dcirCompact, irBenCompact, mcoCompact, irImbCompact, outputPathRoot)

    dcirCompact.unpersist
    mcoCompact.unpersist
    irBenCompact.unpersist
    irImbCompact.unpersist

    None
  }
}