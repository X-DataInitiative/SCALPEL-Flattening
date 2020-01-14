// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.Flattening._
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.ConfigUtils
import fr.polytechnique.cmap.cnam.utilities.reporting.{MainMetadata, MetadataReporter}

object FlatteningJoinOnly extends Main {

  def appName = "FlatteningJoinOnly"

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()

    val conf: FlatteningConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))
    if (conf.autoBroadcastJoinThreshold.nonEmpty) {
      val newThresholdValue = ConfigUtils.byteStringAsBytes(conf.autoBroadcastJoinThreshold.get)
      if (newThresholdValue > 0)
        sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", newThresholdValue.toString)
    }


    logger.info("skip converting csv to parquet")
    //val metaConvert = saveCSVTablesAsParquet(sqlContext, conf)

    logger.info("begin flattening")
    logger.info(sqlContext.getConf("spark.sql.shuffle.partitions"))
    val t0 = System.nanoTime()
    val metaJoin = joinSingleTablesToFlatTable(sqlContext, conf)

    logger.info("finished flattening")
    val t1 = System.nanoTime()
    logger.info("Flattening Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), metaJoin)
    val metadataJson: String = metadata.toJsonString()

    MetadataReporter
      .writeMetaData(metadataJson, "metadata_flattening_" + format.format(startTimestamp) + ".json", argsMap("env"))

    None
  }
}
