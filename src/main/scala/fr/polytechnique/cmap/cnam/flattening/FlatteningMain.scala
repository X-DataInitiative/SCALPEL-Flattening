package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.utilities.DFUtils

object FlatteningMain extends Main {

  def appName = "Flattening"

  def saveCSVTablesAsParquet(sqlContext: SQLContext): Unit = {

    // Generate schemas from csv
    val columnsTypeMap: Map[String, List[(String, String)]] = FlatteningConfig.columnTypes


    FlatteningConfig.partitionsList.foreach {
      config: ConfigPartition =>
        val t0 = System.nanoTime()
        logger.info("converting table " + config.name)
        val columnsType = columnsTypeMap(config.name).toMap

        val rawTable = DFUtils.readCSV(sqlContext, config.inputPaths)
        val typedTable = DFUtils.applySchema(rawTable, columnsType, config.dateFormat)

        typedTable.write.parquet(config.output)
        val t1 = System.nanoTime()
        logger.info("Duration  " + (t1 - t0)/Math.pow(10,9) + " sec")

    }
  }

  def computeFlattenedFiles(sqlContext: SQLContext, configs: List[Config]): Unit = {
    configs.foreach{config =>
      logger.info("begin flattening " + config.getString("name"))
      new FlatTable(sqlContext, config).writeAsParquet}
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    logger.info("begin converting csv to parquet")
    saveCSVTablesAsParquet(sqlContext)

    logger.info("begin flattening")
    val t0 = System.nanoTime()
    computeFlattenedFiles(sqlContext, FlatteningConfig.joinTablesConfig)

    logger.info("finished flattening")
    val t1 = System.nanoTime()
    logger.info("Flattening Duration  " + (t1 - t0)/Math.pow(10,9) + " sec")

    None
  }
}
