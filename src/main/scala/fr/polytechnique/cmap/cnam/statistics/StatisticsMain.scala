package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.DFUtils.readParquet

case class TableSchema(tableName: String, columnTypes: Map[String, String])

object StatisticsMain extends Main {

  override def appName = "Statistics"

  def computeSingleTableStats(
      singleTableData: DataFrame,
      centralTableName: String,
      singleConf: SingleTableConfig): DataFrame = {

    logger.info(s"Computing Statistics on the single table: ${singleConf.tableName}")
    println(singleConf)

    import CustomStatistics.Statistics
    import DataFrameHelper._

    val prefixedData = if(singleConf.tableName != centralTableName)
      singleTableData.prefixColumnNames(singleConf.tableName, "__")
    else
      singleTableData

    prefixedData.customDescribe(distinctOnly = true)
  }

  def describeFlatTable(data: DataFrame, flatConf: FlatTableConfig): Unit = {

    import CustomStatistics.Statistics

    logger.info(s"Computing Statistics on the flat table: ${flatConf.tableName}")
    println(flatConf)

    val flatTableStats = data.drop("year").customDescribe().persist()

    flatTableStats.write.parquet(flatConf.outputStatPath + "/flat_table")

    if(flatConf.singleTables.nonEmpty) {
      val singleTablesStats = flatConf.singleTables.map { singleTableConf =>
        val singleTableData = readParquet(data.sqlContext, singleTableConf.inputPath)
        computeSingleTableStats(singleTableData, flatConf.centralTable, singleTableConf)
      }.reduce(_.union(_)).persist()

      val diff = flatTableStats
        .select(singleTablesStats.columns.map(col): _*)
        .except(singleTablesStats)

      singleTablesStats.write.parquet(flatConf.outputStatPath + "/single_tables")
      diff.write.parquet(flatConf.outputStatPath + "/diff")
    }
  }

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    import DataFrameHelper.ImplicitDF
    // Compute and save stats for old flattening
    if(StatisticsConfig.describeOldFlatTable) {

      StatisticsConfig.oldFlatConfig.foreach { conf =>

        val tablesSchema: List[TableSchema] = FlatteningConfig.columnTypes.map(x =>
          TableSchema(x._1, x._2.toMap)).toList

        val oldFlatData = readParquet(sqlContext, conf.inputPath)
          .drop("key")
          .changeColumnNameDelimiter
          .changeSchema(tablesSchema, conf.centralTable, conf.dateFormat)

        describeFlatTable(oldFlatData, conf)
      }
    }

    // Compute and save stats for main flattening
    StatisticsConfig.mainFlatConfig.foreach { conf =>
      val flatData = readParquet(sqlContext, conf.inputPath)
      describeFlatTable(flatData, conf)
    }

    None
  }
}
