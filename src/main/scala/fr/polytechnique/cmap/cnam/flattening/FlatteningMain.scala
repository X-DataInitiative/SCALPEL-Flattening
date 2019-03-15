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

  //Création du metadata du flattening
  def report (conf: FlatteningConfig): Unit = {
    val operationsMetadata = mutable.Buffer[OperationMetadata]()
    val startTimestamp = new java.util.Date()
    val format = new java.text.SimpleDateFormat("yyyy_MM_dd_HH_mm_ss")


    import scala.collection.mutable.ListBuffer
    //Nom des tables en entrée
    var NamesInputTables = new ListBuffer[String]()
    //Chemin des tables en entrée
    var PathsInputTables = new ListBuffer[String]()
    //Nom des tables en sortie
    var NamesOutputTables = new ListBuffer[String]()
    //Chemin de sortie
    val PathOutput = conf.flatTablePath
    //Partition des tables
    var PartitionTables = new ListBuffer[String]()
    //Format des dates des tables
    var DateTables = new ListBuffer[String]()

    logger.info("Main - FlatteningConfig flattablepath :" + conf.flatTablePath)
    logger.info("Main - FlatteningConfig save :" + conf.singleTableSaveMode)


    for (x <- conf.joinTableConfigs) {
      /*RECUPERATION DES NOMS DE TABLE EN SORTIE*/
      logger.info("Main - JoinTableConfig Noms de table en sortie :" + x.name)
      NamesOutputTables += x.name

      /*Affichage DE LA STRATEGIE DE PARTITION*/
      logger.info("Main - JoinTableConfig partition :" + x.monthlyPartitionColumn)

      /*RECUPERATION DES NOMS DE TABLE EN ENTREE*/
      logger.info("Main - JoinTableConfig input :" + x.mainTableName)
      NamesInputTables += x.mainTableName
      for (y <- x.tablesToJoin) {
        logger.info("Main - JoinTableConfig inputtojoin :" + y)
        NamesInputTables += y
      }

      /*Affichage DES CLES DE JOINTURES*/
      for (z <- x.joinKeys) {
        logger.info("Main - JoinTableConfig keysjoin :" + z)
      }
    }


    /*PARCOURS DE CHAQUE TABLE*/
    conf.partitions.foreach {
      config: ConfigPartition =>
        logger.info("Main - ConfigPartition name :" + config.name)
        logger.info("Main - ConfigPartition partitionColumn :" + config.partitionColumn)
        logger.info("Main - ConfigPartition dateformat :" + config.dateFormat)
        PartitionTables += config.partitionColumn.toString
        DateTables += config.dateFormat

        /*RECUPERATION DES CHEMINS DE TABLE EN ENTREE*/
        for (x <- config.inputPaths) {
          logger.info("Main - ConfigPartition path :" + x)
          PathsInputTables += x
        }
        /*RECUPERATION DU CHEMIN DE TABLE APLATIE*/
        logger.info("Main - ConfigPartition output :" + config.output)
    }
    logger.info("Main - Sortie du parcours")

    logger.info("Ecriture metadata")
    operationsMetadata += {
      OperationReporter.report(
        NamesInputTables.toList,
        PartitionTables.toList,
        DateTables.toList,
        PathsInputTables.toList,
        NamesOutputTables.toList,
        Path(PathOutput),
        conf.singleTableSaveMode
      )
    }

    // Write Metadata
    val metadata = MainMetadata(this.getClass.getName, startTimestamp, new java.util.Date(), operationsMetadata.toList)
    val metadataJson: String = metadata.toJsonString()

    new PrintWriter("metadata_flattening_" + format.format(startTimestamp) + ".json") {
      write(metadataJson)
      close()
    }
    logger.info("Fin ecriture metadata")
  }
}
