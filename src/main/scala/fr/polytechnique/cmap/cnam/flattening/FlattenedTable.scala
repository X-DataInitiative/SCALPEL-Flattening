package fr.polytechnique.cmap.cnam.flattening

/**
  * Created by admindses on 20/02/2017.
  */

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import org.apache.spark.sql.functions._

class FlattenedTable(config: Config, sqlContext: SQLContext) {

  val mainTable = joinTable(sqlContext.read.parquet(config.mainTablePath),config.mainTableKey)
  val tablesToJoin = config.tablesToJoin.map(x => joinTable(sqlContext.read.parquet(x.pathTablesToJoin),x.foreignKeys))
  val outputPath = config.outputPath

  def joinTables(mainTable: joinTable, tablesToJoin: List[joinTable]) = {
    tablesToJoin.foldLeft(mainTable)( (acc: joinTable, other: joinTable) =>
      joinTable(acc.df.join(other.df, mainTable.foreighKey,"leftouter"), List("")))
  }
  lazy val flatTable = joinTables(mainTable,tablesToJoin)



  def saveJoinTable(): Unit = flatTable
    .partitionByYear(config.yearPartitionCols)
    .partitionByMonth(config.monthsPartitionCols)
    .df.write.parquet(outputPath)

  implicit class joinTableUtilities(jt: joinTable) {
    def partitionByYear(colName: String): joinTable = {
      if (colName != "")
        joinTable(jt.df.withColumn("yearPartitionCol", year(jt.df.col(colName))).repartition(jt.df.col("yearPartitionCol")), jt.foreighKey)
      jt
    }

    def partitionByMonth(colName: String): joinTable = {
      if (colName != "")
        joinTable(jt.df.withColumn("monthPartitionCol", month(jt.df.col(colName))).repartition(jt.df.col("monthPartitionCol")), jt.foreighKey)
      jt
    }

  }
}
case class joinTable(df:DataFrame, foreighKey:List[String]){
}
