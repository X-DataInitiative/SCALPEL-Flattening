package fr.polytechnique.cmap.cnam.flattening

/**
  * Created by admindses on 20/02/2017.
  */

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

class FlattenedTable(config: Config, sqlContext: SQLContext) {
  import sqlContext.implicits._
  val mainTable = joinTable(config.mainTableName, sqlContext.read.parquet(config.mainTablePath), config.mainTableKey)
  val tablesToJoin = config.tablesToJoin.map(x => joinTable(x.tableToJoinName, sqlContext.read.parquet(x.pathTablesToJoin), x.foreignKeys))
  val outputPath = config.outputPath

  def joinTables(mainTable: joinTable, tablesToJoin: List[joinTable]) : joinTable = {
    tablesToJoin.foldLeft(mainTable)( (acc: joinTable, other: joinTable) =>
      joinTable(config.name, acc.df.join(other.addPrefix().df, mainTable.foreighKey,"left_outer"), List("")))

  }
  lazy val flatTable = joinTables(mainTable,tablesToJoin)


  def saveJoinTable(): Unit = {
    flatTable
      .partitionByYear(config.yearPartitionCols)
      .flatMap(jt => jt.partitionByMonth(config.monthsPartitionCols))
      .map(jt => jt.df.write.parquet(outputPath + "/" + jt.name))
  }
  implicit class joinTableUtilities(jt: joinTable) {
    def partitionByYear(colName: String): List[joinTable] = {
      if (jt.df.columns.contains(colName)) {
        Logger.getLogger("").info("partition by year")
        val filterColumn: Column = jt.df(colName)
        val years = jt.df.select(year(filterColumn)).distinct.collect().map(_.getInt(0))
        years.map(y => joinTable(jt.name + "_" + y, jt.df.filter(year(filterColumn) === y), jt.foreighKey)).toList
      }
      else
        List(jt)
    }

    def partitionByMonth(colName: String): List[joinTable] = {
      if (jt.df.columns.contains(colName)) {
        Logger.getLogger("").info("partition by month")
        val filterColumn = jt.df(config.monthsPartitionCols)
        val months =  jt.df.select(month(filterColumn)).distinct.collect().map(_.getInt(0))
        months.map(mt => joinTable(jt.name + "_" + mt, jt.df.filter(month(filterColumn) === mt), jt.foreighKey)).toList
      }
      else
        List(jt)
    }
    def addPrefix(): joinTable = {
      val dfRenamed = jt.df.toDF(jt.df.columns.map(x =>
        if (!jt.foreighKey.contains(x))
        {jt.name+ "_" + x}
        else {x})
        : _*)
      joinTable(jt.name, dfRenamed, jt.foreighKey)
    }
  }
}
case class joinTable(name: String, df:DataFrame, foreighKey:List[String]){
}
