package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.Flattening.joinSingleTablesToFlatTable
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import org.apache.spark.sql.{Dataset, SQLContext}

object FlatteningJoinOnly extends Main {

  def appName = "FlatteningJoinOnly"

//  def saveCSVTablesAsParquet(sqlContext: SQLContext, conf: FlatteningConfig): Unit = {
//
//    // Generate schemas from csv
//    val columnsTypeMap: Map[String, List[(String, String)]] = conf.columnTypes
//
//    conf.partitions.filter(_.saveSingleTable).foreach {
//      config: ConfigPartition =>
//        val t0 = System.nanoTime()
//        logger.info("converting table " + config.name)
//        val columnsType = columnsTypeMap(config.name).toMap
//
//        val rawTable = readCSV(sqlContext, config.inputPaths)
//        val typedTable = applySchema(rawTable, columnsType, config.dateFormat)
//        //Do not partition data with a column including only few values
//        //it will cause data skew and reduce the performance when huge data comes
//        if (config.partitionColumn.isDefined)
//          typedTable.writeParquet(config.output, config.partitionColumn.get)(config.singleTableSaveMode)
//        else
//          typedTable.writeParquet(config.output)(config.singleTableSaveMode)
//
//        val t1 = System.nanoTime()
//        logger.info("Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")
//
//    }
//  }

  def computeFlattenedFiles(sqlContext: SQLContext, conf: FlatteningConfig): Unit = {
    conf.joinTableConfigs.filter(_.saveFlatTable).foreach { config =>
      logger.info("begin flattening " + config.name)
      val metaJoin = joinSingleTablesToFlatTable(sqlContext, conf)
    }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val conf: FlatteningConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))


    logger.info("considering csv already converted to parquet!")
    //saveCSVTablesAsParquet(sqlContext, conf)

    logger.info("begin flattening directly")
    logger.info(sqlContext.getConf("spark.sql.shuffle.partitions"))
    val t0 = System.nanoTime()
    computeFlattenedFiles(sqlContext, conf)

    logger.info("finished flattening")
    val t1 = System.nanoTime()
    logger.info("Flattening Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    None
  }
}
