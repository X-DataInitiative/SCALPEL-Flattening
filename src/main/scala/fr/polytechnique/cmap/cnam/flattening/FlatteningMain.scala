package fr.polytechnique.cmap.cnam.flattening

import java.io.PrintWriter

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.ConfigUtils
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import fr.polytechnique.cmap.cnam.utilities.reporting._
import org.apache.spark.sql.{Dataset, SQLContext}

import scala.collection.mutable

object FlatteningMain extends Main {

  def appName = "Flattening"

  def saveCSVTablesAsParquet(sqlContext: SQLContext, conf: FlatteningConfig): Unit = {

    // Generate schemas from csv
    val columnsTypeMap: Map[String, List[(String, String)]] = conf.columnTypes

    conf.partitions.filter(_.saveSingleTable).foreach {
      config: ConfigPartition =>
        val t0 = System.nanoTime()
        logger.info("converting table " + config.name)
        val columnsType = columnsTypeMap(config.name).toMap

        val rawTable = readCSV(sqlContext, config.inputPaths)
        val typedTable = applySchema(rawTable, columnsType, config.dateFormat)
        //Do not partition data with a column including only few values
        //it will cause data skew and reduce the performance when huge data comes
        if (config.partitionColumn.isDefined)
          typedTable.writeParquet(config.output, config.partitionColumn.get)(config.singleTableSaveMode)
        else
          typedTable.writeParquet(config.output)(config.singleTableSaveMode)

        val t1 = System.nanoTime()
        logger.info("Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    }
  }

  def computeFlattenedFiles(sqlContext: SQLContext, conf: FlatteningConfig): Unit = {
    conf.joinTableConfigs.filter(_.saveFlatTable).foreach { config =>
      logger.info("begin flattening " + config.name)
      new FlatTable(sqlContext, config).writeAsParquet()
    }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    val startTimestamp = new java.util.Date()
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
    computeFlattenedFiles(sqlContext, conf)

    logger.info("finished flattening")
    val t1 = System.nanoTime()
    logger.info("Flattening Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    logger.info("begin report")
    report(conf,startTimestamp)
    logger.info("finished report")

    None
  }

  //Create Metadata Flattening
  def report (conf: FlatteningConfig, startTimestamp: java.util.Date): Unit = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()
    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")

    //Output Table
    var outputTable : String = ""
    //Output Path
    val outputPath = conf.flatTablePath

    import scala.collection.mutable.ListBuffer
    //Table Input Name
    val namesInputTables = new ListBuffer[String]()
    //Table Input Path
    val pathsInputTables = new ListBuffer[String]()
    //Partition Table
    var partitionTables = None: Option[String]
    //Format Date
    var dateTables : String = ""
    //Table Join Keys
    val joinKeys = new ListBuffer[String]()

    //Single Tables
    val inputTables = new ListBuffer[InputTable]()


    for (table <- conf.joinTableConfigs) {
      outputTable = table.name
      namesInputTables += table.mainTableName
      namesInputTables ++= table.tablesToJoin

      for (table <- namesInputTables.toList) {
          conf.partitions.foreach {
            config: ConfigPartition =>
              if (table.equals(config.name)) {
                partitionTables = config.partitionColumn
                dateTables = config.dateFormat

                /*Input Table Path*/
                pathsInputTables ++= config.inputPaths
              }
          }
        inputTables += InputTable(table, partitionTables, dateTables.toString, pathsInputTables.toList)
        pathsInputTables.clear()
      }

      /*Join Keys*/
      joinKeys ++= table.joinKeys

      operationsMetadata += {
        OperationReporter.report(
          outputTable,
          outputPath,
          inputTables.toList,
          joinKeys.toList
        )
      }

      namesInputTables.clear()
      inputTables.clear()
    }

    logger.info("Write metadata")

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata_flattening_" + format.format(startTimestamp) + ".json") {
      write(metadataJson)
      close()
    }
    logger.info("End write metadata")
  }
}
