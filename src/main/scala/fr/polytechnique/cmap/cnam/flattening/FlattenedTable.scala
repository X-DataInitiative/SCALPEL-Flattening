package fr.polytechnique.cmap.cnam.flattening

import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.functions._
/**
  * Created by admindses on 20/02/2017.
  */


class FlattenedTable(val name: String, mainTable: Table, tablesToJoin: List[Table], val path: String, val partitionColumn: String)
{
    def join: Table = tablesToJoin.foldLeft(mainTable)( (acc: Table, other: Table) =>
      new Table(name, acc.df.join(other.addPrefix.df, mainTable.foreignKey,"left_outer"), mainTable.foreignKey))

    lazy val table = join

    def writeToParquet(): Unit =
//      partitionColumn match {
    //      case "" => table.df.write.mode(SaveMode.Overwrite).parquet(path + "/" + table.name)
    //      case "year" => table.partitionByYear(partitionColumn).foreach(tabl => tabl.df.write.mode(SaveMode.Overwrite).parquet(path + "/" + tabl.name))
    //      case _ => table.df.withColumn("year",year(table.df.col(partitionColumn))).write.mode(SaveMode.Overwrite).partitionBy(partitionColumn).parquet(path + "/" + table.name)
    //    }
  table.partitionByYear(partitionColumn).foreach(tabl => tabl.df.write.mode(SaveMode.Overwrite).parquet(path + "/" + tabl.name))
}

object FlattenedTable {

  import FlatteningConfig.JoinConfig

  def build(config: Config, sqlContext: SQLContext): FlattenedTable = {
    val mainTable: Table = new Table(config.mainTableName, sqlContext.read.option("mergeSchema","true").parquet(config.inputPath + "/" + config.mainTableName), (config.yearPartitionCols :: config.foreignKeys).distinct)
    val tablesToJoin: List[Table] = config.tablesToJoin.map(tableName =>
      new Table(tableName, sqlContext.read.option("mergeSchema","true").parquet(config.inputPath + "/" + tableName), (config.yearPartitionCols :: config.foreignKeys).distinct)
    )
    val partitionColumn = config.yearPartitionCols
    val outputPath: String = config.outputJoinPath
    val name = config.nameFlatTable
    new FlattenedTable(name, mainTable, tablesToJoin, outputPath, partitionColumn)
  }

}