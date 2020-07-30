// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.Flattening.saveCSVTablesAsSingleTables
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.load
import fr.polytechnique.cmap.cnam.utilities.reporting.OperationMetadata

object FlatteningMainConvert extends Main {
  override def appName: String = "Flattening : CSV to Parquet"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {

    val conf: FlatteningConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))

    logger.info("begin converting csv to parquet")
    val t0 = System.nanoTime()

    val map = saveCSVTablesAsSingleTables(sqlContext, conf)
      .map(operationMetadata => operationMetadata.outputTable -> operationMetadata).toMap

    OperationMetadata.serialize(argsMap("meta_bin"), map)

    logger.info("convert finished")
    val t1 = System.nanoTime()
    logger.info("Convert Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    None
  }

}
