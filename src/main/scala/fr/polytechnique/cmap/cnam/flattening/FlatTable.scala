package fr.polytechnique.cmap.cnam.flattening

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.JoinTableConfig

class FlatTable(sqlContext: SQLContext, config: JoinTableConfig) {

  val inputBasePath: String = config.inputPath
  val mainTable: Table = Table.build(sqlContext, inputBasePath, config.mainTableName)
  val tablesToJoin: List[Table] = config.tablesToJoin.map(
    tableName =>
      Table.build(sqlContext, inputBasePath, tableName)
  )
  val outputBasePath: String = config.flatOutputPath
  val foreignKeys: List[String] = config.joinKeys
  val tableName: String = config.name
  val monthlyPartitionColumn: Option[String] = config.monthlyPartitionColumn

  def flatTablePerYear: Array[Int] = mainTable.getYears

  def joinByYear(year: Int): Table = {
    val name = s"$tableName/year=$year"
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .foldLeft(mainTable.filterByYear(year))(joinFunction)

    new Table(name, joinedDF)
  }

  def joinByYearAndDate(year: Int, month: Int, monthCol: String): Table = {
    val name = s"$tableName/year=$year"
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol))
      .foldLeft(mainTable.filterByYearAndMonth(year, month, monthCol))(joinFunction)

    new Table(name, joinedDF)
  }

  val joinFunction: (DataFrame, DataFrame) => DataFrame = (accumulator, tableToJoin) => {
    val result = accumulator.join(tableToJoin, foreignKeys, "left_outer").persist()
    Logger.getLogger(getClass).info(s"Joined table count: ${result.count()}")
    accumulator.unpersist()
    result
  }

  def logger: Logger = Logger.getLogger(getClass)

  def writeTable(table: Table): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)
    val t0 = System.nanoTime()

    val flatTable = table.df.persist()
    val nbLinesFlatTable = flatTable.count()
    logger.info("   Number of lines : " + nbLinesFlatTable)

    val t1 = System.nanoTime()
    logger.info("   Flattening Duration " + (t1 - t0) / Math.pow(10, 9) + " sec")

    flatTable.write
      .mode(SaveMode.Append)
      .parquet(outputBasePath + "/" + table.name)
    flatTable.unpersist()
    val t2 = System.nanoTime()
    logger.info("   writing duration " + table.name + (t2 - t1) / Math.pow(10, 9) + " sec")
  }

  def writeAsParquet(): Unit = {
    flatTablePerYear
      .foreach {
        year =>
          if (monthlyPartitionColumn.isDefined) {
            logger.info("Join by year and month: " + year)
            Range(1, 13).foreach {
              month =>
                logger.info("Month: " + month)
                val joinedTable = joinByYearAndDate(year, month, monthlyPartitionColumn.get)
                writeTable(joinedTable)
            }
          }
          else {
            logger.info("Join by year : " + year)
            val joinedTable = joinByYear(year)
            writeTable(joinedTable)
          }
      }
  }

}
