package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.Flattening.saveCSVTablesAsParquet
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.load

object FlatteningMainConvert extends Main {
  override def appName: String = "flattening convert"

  override def run(
    sqlContext: SQLContext,
    argsMap: Map[String, String]): Option[Dataset[_]] = {

    val conf: FlatteningConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))

    logger.info("begin converting csv to parquet")
    val t0 = System.nanoTime()

    saveCSVTablesAsParquet(sqlContext, conf)

    logger.info("convert finished")
    val t1 = System.nanoTime()
    logger.info("Convert Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    None
  }

}
