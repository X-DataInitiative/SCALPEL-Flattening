package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.Flattening.joinSingleTablesToFlatTable
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.load
import fr.polytechnique.cmap.cnam.utilities.ConfigUtils

object FlatteningMainJoin extends Main {
  override def appName: String = "flattening join"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {

    val conf: FlatteningConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))
    if (conf.autoBroadcastJoinThreshold.nonEmpty) {
      val newThresholdValue = ConfigUtils.byteStringAsBytes(conf.autoBroadcastJoinThreshold.get)
      if (newThresholdValue > 0)
        sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", newThresholdValue.toString)
    }

    logger.info("begin flattening")
    val t0 = System.nanoTime()
    joinSingleTablesToFlatTable(sqlContext, conf)

    logger.info("finished flattening")
    val t1 = System.nanoTime()
    logger.info("Flattening Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    None
  }

}
