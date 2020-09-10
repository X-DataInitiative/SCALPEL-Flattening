package fr.polytechnique.cmap.cnam.flattening.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.JoinTableConfig
import fr.polytechnique.cmap.cnam.flattening.tables.{AnyTable, Table}
import fr.polytechnique.cmap.cnam.utilities.DFUtils._

abstract class Joiner[A <: AnyTable, B <: AnyTable](sqlContext: SQLContext, config: JoinTableConfig, format: String = "parquet") {

  def logger: Logger = Logger.getLogger(getClass)

  def mainTable: Table[A]

  val tablesToJoin: List[Table[A]]

  val referencesToJoin: List[(Table[A], FlatteningConfig.Reference)]

  def joinRefs(table: Table[B]): Table[B]

  def joinByYear(name: String, year: Int): Table[B]

  def joinByYearAndDate(name: String, year: Int, month: Int, monthCol: String): Table[B]

  def writeTable(table: Table[B], outputPath: String, saveMode: String): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    val t0 = System.nanoTime()
    table.df.write(outputPath + "/" + table.name)(saveMode, format)
    val t1 = System.nanoTime()
    logger.info(s"   writing duration ${table.name} ${(t1 - t0) / Math.pow(10, 9)} sec")
  }

  /*
 * if monthlyPartitionColumn is set, the data will be partitioned in year and month
 * if not, the data will be partitioned in year
 * We can output the data in specifying its months and years
 */
  def join: Unit = {

    val monthlyPartitionColumn: Option[String] = config.monthlyPartitionColumn

    mainTable
      .getYears
      .filter(year => config.onlyOutput.isEmpty || config.onlyOutput.exists(_.year == year))
      .foreach {
        year =>
          if (monthlyPartitionColumn.isDefined) {
            logger.info("Join by year and month: " + year)
            val filteredMonths = config.onlyOutput.find(_.year == year) match {
              case None => List.empty[Int]
              case Some(thisYear) => thisYear.months
            }
            Range(1, 13).filter(filteredMonths.isEmpty || filteredMonths.contains(_)).foreach {
              month =>
                logger.info("Month: " + month)
                val name = s"${config.name}/year=$year/month=$month"
                writeTable(joinRefs(joinByYearAndDate(name, year, month, monthlyPartitionColumn.get)), config.flatOutputPath.get, config.flatTableSaveMode)
            }
          }
          else {
            logger.info("Join by year : " + year)
            val name = s"${config.name}/year=$year"
            writeTable(joinRefs(joinByYear(name, year)), config.flatOutputPath.get, config.flatTableSaveMode)
          }
      }
  }

}
