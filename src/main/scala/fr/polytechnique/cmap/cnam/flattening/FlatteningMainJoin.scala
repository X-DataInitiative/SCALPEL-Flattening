package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.Flattening.joinSingleTablesToFlatTable
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.load
import fr.polytechnique.cmap.cnam.utilities.ConfigUtils
import fr.polytechnique.cmap.cnam.utilities.reporting.{MainMetadata, MetadataReporter, OperationMetadata}

object FlatteningMainJoin extends Main {
  override def appName: String = "Flattening : Tables Join"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {

    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    val startTimestamp = new java.util.Date()

    val conf: FlatteningConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))
    val operationsMetadata = OperationMetadata.deserialize(argsMap("meta_bin"))
    val loadedConf = conf.copy(join =
      conf.joinTableConfigs.map {
        config =>
          operationsMetadata.get(config.mainTableName) match {
            case None => config
            case Some(op) => config.copy(inputPath = Some(op.outputPath))
          }
      })

    if (loadedConf.autoBroadcastJoinThreshold.nonEmpty) {
      val newThresholdValue = ConfigUtils.byteStringAsBytes(conf.autoBroadcastJoinThreshold.get)
      if (newThresholdValue > 0)
        sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold", newThresholdValue.toString)
    }

    logger.info("begin flattening")
    val t0 = System.nanoTime()
    val meta = joinSingleTablesToFlatTable(sqlContext, loadedConf)

    logger.info("finished flattening")
    val t1 = System.nanoTime()
    logger.info("Flattening Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), (operationsMetadata.values ++ meta).toList)
    val metadataJson: String = metadata.toJsonString()

    MetadataReporter
      .writeMetaData(metadataJson, "metadata_flattening_" + format.format(startTimestamp) + ".json", argsMap("env"))

    None
  }

}
