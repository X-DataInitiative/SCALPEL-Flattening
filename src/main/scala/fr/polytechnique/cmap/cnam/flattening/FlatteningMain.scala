package fr.polytechnique.cmap.cnam.flattening

import java.io.PrintWriter

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import fr.polytechnique.cmap.cnam.utilities.Path
import fr.polytechnique.cmap.cnam.utilities.reporting._
import org.apache.spark.sql.{Dataset, SQLContext}

import scala.collection.mutable
import scala.util.control.Breaks

object FlatteningMain extends Main {

  def appName = "Flattening"

  def saveCSVTablesAsParquet(sqlContext: SQLContext, conf: FlatteningConfig): Unit = {

    // Generate schemas from csv
    val columnsTypeMap: Map[String, List[(String, String)]] = conf.columnTypes

    conf.partitions.foreach {
      config: ConfigPartition =>
        val t0 = System.nanoTime()
        logger.info("converting table " + config.name)
        val columnsType = columnsTypeMap(config.name).toMap

        val rawTable = readCSV(sqlContext, config.inputPaths)
        val typedTable = applySchema(rawTable, columnsType, config.dateFormat)

        if (config.partitionColumn.isDefined)
          typedTable.writeParquet(config.output, config.partitionColumn.get)(conf.singleTableSaveMode)
        else
          typedTable.writeParquet(config.output)(conf.singleTableSaveMode)

        val t1 = System.nanoTime()
        logger.info("Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

    }
  }

  def computeFlattenedFiles(sqlContext: SQLContext, conf: FlatteningConfig): Unit = {
    conf.joinTableConfigs.foreach { config =>
      logger.info("begin flattening " + config.name)
      new FlatTable(sqlContext, config).writeAsParquet()
    }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    val conf: FlatteningConfig = load(argsMap.getOrElse("conf", ""), argsMap("env"))

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
    report(conf)
    logger.info("finished report")

    None
  }

  //Create Metadata Flattening
  def report (conf: FlatteningConfig): Unit = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()
    val startTimestamp = new java.util.Date()
    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")


    //Output Table
    var OutputTable : String = ""
    //Output Path
    val OutputPath = conf.flatTablePath

    import scala.collection.mutable.ListBuffer
    //Table Input Name
    var NamesInputTables = new ListBuffer[String]()
    //Table Input Path
    var PathsInputTables = new ListBuffer[String]()
    //Partition Table
    var PartitionTables : String = ""
    //Format Date
    var DateTables : String = ""
    //Table Join Keys
    var JoinKeys = new ListBuffer[String]()

    //Single Tables
    val InputTables = new ListBuffer[InputTable]()

    logger.info("Main - FlatteningConfig flattablepath :" + conf.flatTablePath)
    logger.info("Main - FlatteningConfig save :" + conf.singleTableSaveMode)

    for (x <- conf.joinTableConfigs) {
      /*Output Table*/
      logger.info("Main - JoinTableConfig Noms de table en sortie :" + x.name)
      OutputTable = x.name

      /*Partition Column*/
      logger.info("Main - JoinTableConfig partition :" + x.monthlyPartitionColumn)

      val loop = new Breaks;

      /*Input Table Name*/
      logger.info("Main - JoinTableConfig input :" + x.mainTableName)
      NamesInputTables += x.mainTableName
      for(w <- x.tablesToJoin) {
        NamesInputTables += w
      }

      for (y <- NamesInputTables.toList) {
        logger.info("Main - JoinTableConfig inputtojoin :" + y)

        loop.breakable {
          conf.partitions.foreach {
            config: ConfigPartition =>
              if (y.equals(config.name)) {
                logger.info("Main - ConfigPartition name :" + config.name)
                logger.info("Main - ConfigPartition partitionColumn :" + config.partitionColumn)
                logger.info("Main - ConfigPartition dateformat :" + config.dateFormat)
                PartitionTables = config.partitionColumn.toString
                DateTables = config.dateFormat

                /*Input Table Path*/
                for (x <- config.inputPaths) {
                  logger.info("Main - ConfigPartition path :" + x)
                  PathsInputTables += x
                }
                /*Output*/
                logger.info("Main - ConfigPartition output :" + config.output)

                InputTables += new InputTable(y, PartitionTables, DateTables, PathsInputTables.toList)
                PathsInputTables.clear()
                loop.break()
              }
          }
        }
      }

      /*Join Keys*/
      for (z <- x.joinKeys) {
        logger.info("Main - JoinTableConfig keysjoin :" + z)
        JoinKeys += z
      }

      operationsMetadata += {
        OperationReporter.report(
          OutputTable,
          Path(OutputPath),
          InputTables.toList,
          JoinKeys.toList,
          conf.singleTableSaveMode
        )
      }

      NamesInputTables.clear()
      InputTables.clear()
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
