package fr.polytechnique.cmap.cnam.flattening

import java.io.PrintWriter

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import fr.polytechnique.cmap.cnam.utilities.Path
import fr.polytechnique.cmap.cnam.utilities.reporting._
import org.apache.spark.sql.{Dataset, SQLContext}

import scala.collection.mutable

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


  def report (conf: FlatteningConfig): Unit = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()
    val startTimestamp = new java.util.Date()
    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")
    import scala.collection.mutable.ListBuffer
    var NamesInputTables = new ListBuffer[String]()
    var PathsInputTables = new ListBuffer[String]()
    var NamesOutputTables = new ListBuffer[String]()
    val PathOutput = conf.flatTablePath

    /*
    conf.flattablepath = /shared/FALL/staging/flattening/2014_2016/flat_table

    "chemin = /shared/FALL/staging/flattening/2014_2016/flat_table/mco_ce
    "conf.singleTableSaveMode=append"
    "x.name=MCO_CE"
    "x.monthlyPartitionColumn=None"
    "x.mainTableName=MCO_CSTC"
    "x.tablesToJoin=MCO_FMSTC     ET       MCO_FASTC"
    "x.joinKeys=ETA_NUM           ET       SEQ_NUM"


    "config.name=MCO_FMSTC"
    "config.inputPaths=/shared/FALL/raw/2014_2016/T_MCO14FMSTC.CSV"
    "config.output=/shared/FALL/staging/flattening/2014_2016/single_table/MCO_FMSTC/year=2014"

    "config.name=MCO_FMSTC"
    "config.inputPaths=/shared/FALL/raw/2014_2016/T_MCO16FMSTC.CSV"
    "config.output=/shared/FALL/staging/flattening/2014_2016/single_table/MCO_FMSTC/year=2016"

    "config.name=MCO_CSTC"
    "config.inputPaths=/shared/FALL/raw/2014_2016/T_MCO14CSTC.CSV"
    "config.output=/shared/FALL/staging/flattening/2014_2016/single_table/MCO_CSTC/year=2014"

    "config.name=MCO_CSTC"
    "config.inputPaths=/shared/FALL/raw/2014_2016/T_MCO16CSTC.CSV"
    "config.output=/shared/FALL/staging/flattening/2014_2016/single_table/MCO_CSTC/year=2016"

    "config.name=MCO_FASTC"
    "config.inputPaths=/shared/FALL/raw/2014_2016/T_MCO14FASTC.CSV"
    "config.output=/shared/FALL/staging/flattening/2014_2016/single_table/MCO_FASTC/year=2014"

    "config.name=MCO_FASTC"
    "config.inputPaths=/shared/FALL/raw/2014_2016/T_MCO16FASTC.CSV"
    "config.output=/shared/FALL/staging/flattening/2014_2016/single_table/MCO_FASTC/year=2016"
    */

    logger.info("Main - FlatteningConfig flattablepath :" + conf.flatTablePath)
    val chemin = PathOutput.concat("/mco_ce")
    logger.info("Main - FlatteningConfig chemin :" + chemin)
    logger.info("Main - FlatteningConfig save :" + conf.singleTableSaveMode)


    for (x <- conf.joinTableConfigs) {
      /*RECUPERATION DES NOMS DE TABLE EN SORTIE*/
      logger.info("Main - JoinTableConfig Noms de table en sortie :" + x.name)
      NamesOutputTables += x.name

      /*RECUPERATION DE LA STRATEGIE DE PARTITION*/
      logger.info("Main - JoinTableConfig partition :" + x.monthlyPartitionColumn)

      /*RECUPERATION DES NOMS DE TABLE EN ENTREE*/
      logger.info("Main - JoinTableConfig input :" + x.mainTableName)
      NamesInputTables += x.mainTableName
      for (y <- x.tablesToJoin) {
        logger.info("Main - JoinTableConfig inputtojoin :" + y)
        NamesInputTables += y
      }

      /*RECUPERATION DES CLES DE JOINTURES*/
      for (z <- x.joinKeys) {
        logger.info("Main - JoinTableConfig keysjoin :" + z)
      }
    }

    var NamesTables = new ListBuffer[String]()
    var PartitionTables = new ListBuffer[String]()
    var DateTables = new ListBuffer[String]()

    /*PARCOURS DE CHAQUE TABLE*/
    conf.partitions.foreach {
      config: ConfigPartition =>
        logger.info("Main - ConfigPartition name :" + config.name)
        logger.info("Main - ConfigPartition partitionColumn :" + config.partitionColumn)
        logger.info("Main - ConfigPartition dateformat :" + config.dateFormat)
        NamesTables += config.name
        PartitionTables += config.partitionColumn
        DateTables += config.dateFormat

        /*RECUPERATION DES CHEMINS DE TABLE EN ENTREE*/
        for (x <- config.inputPaths) {
          logger.info("Main - ConfigPartition path :" + x)
          PathsInputTables += x
        }
        /*RECUPERATION DU CHEMIN DE TABLE APLATIE*/
        logger.info("Main - ConfigPartition output :" + config.output)
    }
    logger.info("Main - Sortie conf partitions foreach")


    for (z <- NamesOutputTables.toList) {

      val sources = sqlContext.read.parquet(conf.flatTablePath.concat("/"+z.toLowerCase))

      logger.info("Ecriture metadata")

      operationsMetadata += {
        OperationReporter.report(NamesInputTables.toList,
          PathsInputTables.toList,
          NamesOutputTables.toList,
          Path(PathOutput),
          sources,
          conf.singleTableSaveMode
        )
      }
    }

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata_flattening_" + format.format(startTimestamp) + ".json") {
      write(metadataJson)
      close()
    }
  }
}
