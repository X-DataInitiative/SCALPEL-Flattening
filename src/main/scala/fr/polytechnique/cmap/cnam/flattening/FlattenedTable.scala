package fr.polytechnique.cmap.cnam.flattening

/**
  * Created by admindses on 20/02/2017.
  */

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._

class FlattenedTable(config: Config, sqlContext: SQLContext) {

  val mainTable = joinTable(sqlContext.read.parquet(config.mainTablePath),config.mainTableKey)
  val tablesToJoin = config.tablesToJoin.map(x => joinTable(sqlContext.read.parquet(x.pathTablesToJoin),x.foreignKeys))
  val outputPath = config.outputPath

  def joinTables(mainTable: joinTable, tablesToJoin: List[joinTable]) = {
    tablesToJoin.foldLeft(mainTable)( (acc:joinTable,other:joinTable) =>
      joinTable(acc.df.join(other.df, mainTable.foreighKey,"leftouter"), List("")))
  }
  lazy val flatTable = joinTables(mainTable,tablesToJoin)
  def saveJoinTable(): Unit = flatTable.df.write.parquet(outputPath)

}
case class joinTable(df:DataFrame, foreighKey:List[String]){
}