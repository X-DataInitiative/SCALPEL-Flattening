// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.flattening.convert.CSVConverter
import fr.polytechnique.cmap.cnam.flattening.join.{FlatTableJoiner, PMSIFlatTableJoiner, SSRRIPFlatTableJoiner}
import fr.polytechnique.cmap.cnam.utilities.reporting.OperationMetadata

object Flattening {

  def logger: Logger = Logger.getLogger(getClass)

  def saveCSVTablesAsParquet(sqlContext: SQLContext, conf: FlatteningConfig): List[OperationMetadata] = {

    // Generate schemas from csv
    val columnsTypeMap: Schema = conf.columnTypes

    conf.partitions.filter(_.saveSingleTable).groupBy(_.name)
      .map {
        case (_, list: List[ConfigPartition]) =>
          list.map {
            config: ConfigPartition =>
              val t0 = System.nanoTime()
              logger.info("converting table " + config.name)
              CSVConverter.convert(sqlContext, config, columnsTypeMap, conf.fileFormat)
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
   * Iterate on the joinTableConfigs and decide which join logic to apply depending on the product:
   *- if pmsiPatientTableName is empty (we are dealing with DCIR tables) :
   * join all tables to the central table with a `foldleft`
   *- else (pmsiPatientTableName is not empty ie. we are dealing with PMSI tables) :
   *- if joinKeysPatient is empty (for MCO for example) :
   * join mainTable with pmsiPatientTable as a central table,
   * then join successively each of the periphery tables to this central table and concatenate all of these tables as the flat table.
   * -else (for SSR or RIP for example) :
   * it's the same thing as if joinKeysPatient is empty except that for the periphery tables, the set of keys will be
   * the one in joinKeysPatient
   */
  def joinSingleTablesToFlatTable(sqlContext: SQLContext, conf: FlatteningConfig): List[OperationMetadata] = {
    conf.joinTableConfigs.filter(_.saveFlatTable).map { config =>
      if (config.pmsiPatientTableName.isEmpty) {
        logger.info("join table " + config.name + " with Dcir logic")
        new FlatTableJoiner(sqlContext, config, conf.fileFormat).join
        OperationMetadata(config.name, config.flatOutputPath.get, "flat_table",
          config.mainTableName :: (config.tablesToJoin ++ config.refsToJoin.map(_.name)), config.joinKeys)
      }
      else {
        if (config.joinKeysPatient.isEmpty) {
          logger.info("join table " + config.name + " with Pmsi logic")
          new PMSIFlatTableJoiner(sqlContext, config, conf.fileFormat).join
          OperationMetadata(config.name, config.flatOutputPath.get, "flat_table",
            config.mainTableName :: (config.tablesToJoin ++ config.refsToJoin.map(_.name)), config.joinKeys)
        }
        else {
          logger.info("join table " + config.name + " with SSR-RIP logic")
          new SSRRIPFlatTableJoiner(sqlContext, config, conf.fileFormat).join
          OperationMetadata(config.name, config.flatOutputPath.get, "flat_table",
            config.mainTableName :: (config.tablesToJoin ++ config.refsToJoin.map(_.name)), config.joinKeys)
        }
      }
    }
  }

}
