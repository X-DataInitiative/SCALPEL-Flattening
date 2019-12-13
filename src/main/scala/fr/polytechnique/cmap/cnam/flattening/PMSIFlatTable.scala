// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.JoinTableConfig
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{col,lit}

class PMSIFlatTable(sqlContext: SQLContext, config: JoinTableConfig) {

  val inputBasePath: String = config.inputPath.get
  val mainTable: Table = Table.build(sqlContext, inputBasePath, config.mainTableName)
  val pmsiPatientTable: Table = Table.build(sqlContext, inputBasePath, config.pmsiPatientTableName.get)
  val tablesToJoin: List[Table] = config.tablesToJoin.map(
    tableName =>
      Table.build(sqlContext, inputBasePath, tableName)
  )
  val outputBasePath: String = config.flatOutputPath.get
  val foreignKeys: List[String] = config.joinKeys
  val tableName: String = config.name
  val monthlyPartitionColumn: Option[String] = config.monthlyPartitionColumn
  val saveMode: String = config.flatTableSaveMode
  val referencesToJoin: List[(Table, FlatteningConfig.Reference)] = config.refsToJoin.map {
    refConfig => (Table.build(sqlContext, refConfig.inputPath.get, refConfig.name), refConfig)
  }

  /**
   * This method merge two schemas, if a column is not in the schema of the
   * DataFrame, it created the column as empty
   */
  def mergeSchemas(myCols: Set[String], allCols: Set[String]) = {
    allCols.toList.map(x => x match {
      case x if myCols.contains(x) => col(x)
      case _ => lit(null).alias(x)
    })
  }

  /**
   * This method is an amelioration of the union method : if a column exists
   * in one DataFrame and not in the other, it is created as emptys
   */

  def unionWithDifferentSchemas(DF1: DataFrame, DF2: DataFrame): DataFrame = {
    val cols1 = DF1.columns.toSet
    val cols2 = DF2.columns.toSet
    val total = cols1 ++ cols2
    DF1.select(mergeSchemas(cols1, total):_*).union(DF2.select(mergeSchemas(cols2, total):_*))
  }

  def flatTablePerYear: Array[Int] = mainTable.getYears.filter {
    year => config.onlyOutput.isEmpty || config.onlyOutput.exists(_.year == year)
  }

  def joinRefs(table: Table): Table = {
    referencesToJoin.foldLeft(table) {
      case (flatTable, (refTable, refConfig)) =>
        val refDF = refTable.annotate(List.empty[String])
        val cols = refConfig.joinKeysTuples.map {
          case (ftKey, refKey) => flatTable.df.col(ftKey) === refDF.col(refKey)
        }.reduce(_ && _)
        new Table(flatTable.name, flatTable.df.join(refDF, cols, "left"))
    }
  }

  def joinByYear(year: Int): Table = {
    val name = s"$tableName/year=$year"
    val centralTableDF: DataFrame = joinFunction(mainTable.filterByYear(year).drop("year"),
      pmsiPatientTable.filterByYearAndAnnotate(year, foreignKeys))
    tablesToJoin.foreach{table =>
      println(table.name)
      table.df.show()
    }
    centralTableDF.show()
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearAndAnnotate(year, foreignKeys))
      .map(df => joinFunction(centralTableDF, df)).reduce(unionWithDifferentSchemas)
    joinedDF.show()
    new Table(name, joinedDF)
  }

  def joinByYearAndDate(year: Int, month: Int, monthCol: String): Table = {
    val name = s"$tableName/year=$year/month=$month"
    val centralTableDF: DataFrame = joinFunction(mainTable.filterByYearAndMonth(year, month, monthCol).drop("year"),
      pmsiPatientTable.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol))
    val joinedDF = tablesToJoin
      .map(table => table.filterByYearMonthAndAnnotate(year, month, foreignKeys, monthCol))
      .map(table => joinFunction(centralTableDF, table))
      .reduce(unionWithDifferentSchemas)
    new Table(name, joinedDF)
  }

  val joinFunction: (DataFrame, DataFrame) => DataFrame =
    (accumulator, tableToJoin) => accumulator.join(tableToJoin, foreignKeys, "inner")

  def logger: Logger = Logger.getLogger(getClass)

  def writeTable(table: Table): Unit = {

    Logger.getRootLogger.setLevel(Level.ERROR)
    val t0 = System.nanoTime()
    table.df.writeParquet(outputBasePath + "/" + table.name)(saveMode)
    val t1 = System.nanoTime()
    logger.info(s"   writing duration ${table.name} ${(t1 - t0) / Math.pow(10, 9)} sec")
  }

  /*
   * if monthlyPartitionColumn is set, the data will be partitioned in year and month
   * if not, the data will be partitioned in year
   * We can output the data in specifying its months and years
   */
  def writeAsParquet(): Unit = {
    flatTablePerYear
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
                val joinedTable = joinRefs(joinByYearAndDate(year, month, monthlyPartitionColumn.get))
                writeTable(joinedTable)
            }
          }
          else {
            logger.info("Join by year : " + year)
            val joinedTable = joinRefs(joinByYear(year))
            writeTable(joinedTable)
          }
      }
  }

}
