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

              val rawTable = readCSV(sqlContext, config.inputPaths, conf.delimiter)
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

  /**
  Iterate on the joinTableConfigs and decide which join logic to apply depending on the product:
   - if pmsiPatientTableName is empty (we are dealing with DCIR tables) :
        join all tables to the central table with a `foldleft`
   - else (pmsiPatientTableName is not empty ie. we are dealing with PMSI tables) :
        join mainTable with pmsiPatientTable as a central table,
        then join successively each of the periphery tables to this central table and concatenate all of these tables as the flat table.
   */
  def joinSingleTablesToFlatTable(sqlContext: SQLContext, conf: FlatteningConfig): List[OperationMetadata] = {
    conf.joinTableConfigs.filter(_.saveFlatTable).map { config =>
      if (config.pmsiPatientTableName.isEmpty) {
        logger.info("join table " + config.name + " with Dcir logic")
        new FlatTable(sqlContext, config).writeAsParquet()
        OperationMetadata(config.name, config.flatOutputPath.get, "flat_table", config.mainTableName :: config.tablesToJoin, config.joinKeys)
      }
      else {
        if (config.joinKeysPatient.isEmpty) {
          logger.info("join table " + config.name + " with Pmsi logic")
          new PMSIFlatTable(sqlContext, config).writeAsParquet()
          OperationMetadata(config.name, config.flatOutputPath.get, "flat_table", config.mainTableName :: config.tablesToJoin, config.joinKeys)
        }
        else {
          logger.info("join SSR table " + config.name + " with Pmsi logic")
          new SSRRIPFlatTable(sqlContext, config).writeAsParquet()
          OperationMetadata(config.name, config.flatOutputPath.get, "flat_table", config.mainTableName :: config.tablesToJoin, config.joinKeys)
        }
      }
    }
  }

}
