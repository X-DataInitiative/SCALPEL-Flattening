package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.Flattening._
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.ConfigUtils

object FlatteningMain extends Main {

  def appName = "Flattening"

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val conf: FlatteningConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))
    if (conf.autoBroadcastJoinThreshold.nonEmpty) {
      val newThresholdValue = ConfigUtils.byteStringAsBytes(conf.autoBroadcastJoinThreshold.get)
      if (newThresholdValue > 0)
        sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", newThresholdValue.toString)
    }


    logger.info("begin converting csv to parquet")
    saveCSVTablesAsParquet(sqlContext, conf)

    logger.info("begin flattening")
    logger.info(sqlContext.getConf("spark.sql.shuffle.partitions"))
    val t0 = System.nanoTime()
    joinSingleTablesToFlatTable(sqlContext, conf)

    logger.info("finished flattening")
    val t1 = System.nanoTime()
    logger.info("Flattening Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    None
  }
}
