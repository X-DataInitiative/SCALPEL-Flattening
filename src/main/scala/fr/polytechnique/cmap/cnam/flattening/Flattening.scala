// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.flattening.TableColumnsActions._
import fr.polytechnique.cmap.cnam.utilities.DFUtils.{applySchema, readCSV, _}
import fr.polytechnique.cmap.cnam.utilities.reporting.OperationMetadata

object Flattening {

  def logger: Logger = Logger.getLogger(getClass)

  def saveCSVTablesAsParquet(sqlContext: SQLContext, conf: FlatteningConfig): List[OperationMetadata] = {

    // Generate schemas from csv
    val columnsTypeMap: Map[String, List[(String, String)]] = conf.columnTypes

    conf.partitions.filter(_.saveSingleTable).groupBy(_.name)
      .map {
        case (_, list: List[ConfigPartition]) =>
          list.map {
            config: ConfigPartition =>
              val t0 = System.nanoTime()
              logger.info("converting table " + config.name)
              val columnsType = columnsTypeMap(config.name).toMap

              val rawTable = readCSV(sqlContext, config.inputPaths)
              val typedTable = applySchema(rawTable, columnsType, config.dateFormat).processActions(config)
              //Do not partition data with a column including only few values
              //it will cause data skew and reduce the performance when huge data comes
              if (config.partitionColumn.isDefined)
                typedTable.writeParquet(config.output, config.partitionColumn.get)(config.singleTableSaveMode)
              else
                typedTable.writeParquet(config.output)(config.singleTableSaveMode)

              val t1 = System.nanoTime()
              logger.info("Duration  " + (t1 - t0) / Math.pow(10, 9) + " sec")

              config
          }.foldLeft(OperationMetadata("", "", "")) {
            case (op, config) =>
              OperationMetadata(config.name, conf.singleTablePath, "single_table", op.sources ++ config.inputPaths)
          }
      }.toList
  }

  def joinSingleTablesToFlatTable(sqlContext: SQLContext, conf: FlatteningConfig): List[OperationMetadata] = {
    conf.joinTableConfigs.filter(_.saveFlatTable).map { config =>
      logger.info("join table " + config.name)
      new FlatTable(sqlContext, config).writeAsParquet()
      OperationMetadata(config.name, config.flatOutputPath.get, "flat_table",
        config.mainTableName :: (config.tablesToJoin ++ config.refsToJoin.map(_.name)), config.joinKeys)
    }
  }

}
