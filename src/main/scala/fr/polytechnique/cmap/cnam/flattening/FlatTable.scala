package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.flattening.FlatteningMain.logger
import org.apache.log4j.{Level, Logger}

class FlatTable(sqlContext: SQLContext, config: Config) {

  import FlatteningConfig.JoinConfig

  val inputBasePath: String = config.inputPath
  val mainTable: Table = Table.build(sqlContext, inputBasePath, config.mainTableName)
  val tablesToJoin: List[Table] = config.tablesToJoin.map(
    tableName =>
      Table.build(sqlContext, inputBasePath, tableName)
  )
  val outputBasePath: String = config.outputJoinPath
  val foreignKeys: List[String] = config.foreignKeys
  val tableName: String = config.nameFlatTable

  def flatTablePerYear: Array[Table] = mainTable.getYears.map(joinByYear)

  def joinByYear(year: Int): Table = {
    val name = s"$tableName/year=$year"
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .foldLeft(mainTable.filterByYear(year))(joinFunction)

    new Table(name, joinedDF)
  }

  val joinFunction: (DataFrame, DataFrame) => DataFrame = (accumulator, tableToJoin) => {
    accumulator.join(tableToJoin, foreignKeys, "left_outer")
  }

  def writeAsParquet: Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    val logger: Logger = Logger.getLogger(getClass)
    logger.setLevel(Level.INFO)

    flatTablePerYear
      .foreach {
        joinedTable =>
          logger.info("starting join for : " + joinedTable.name)
          val t0 = System.nanoTime()

          val flatTable = joinedTable.df.persist()

          val nbLinesFlatTable = flatTable.count()
          logger.info("   Number of lines : " + nbLinesFlatTable)

          val t1 = System.nanoTime()
          logger.info("   Flattening Duration " + (t1 - t0)/Math.pow(10,9) + " sec")

          flatTable.write
            .mode(SaveMode.Overwrite)
            .parquet(outputBasePath + "/" + joinedTable.name)
          flatTable.unpersist()
          val t2 = System.nanoTime()
          logger.info("   writing duration " +joinedTable.name + (t2 - t1)/Math.pow(10,9) + " sec")
      }
    }



}
