package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.DFUtils.readParquet

case class TableSchema(tableName: String, columnTypes: Map[String, String])

object StatisticsMain extends Main {

  override def appName = "Statistics"

  def computeSingleTableStats(
      singleTableData: DataFrame,
      isCentral: Boolean,
      singleConf: SingleTableConfig,
      joinKeys: List[String] = List()): DataFrame = {

    logger.info(s"Computing Statistics on the single table: ${singleConf.tableName}")
    println(singleConf)

    import DataFrameHelper._
    import DataFrameStatistics.Statistics

    val prefixedData = if(isCentral)
      singleTableData
    else
      // Join keys are removed because their stats are already computed in the central table
      singleTableData.drop(joinKeys: _*).prefixColumnNames(singleConf.tableName, "__")

    prefixedData
      .customDescribe(distinctOnly = true)
      .withColumn("TableName", lit(singleConf.tableName))
  }

  def describeFlatTable(data: DataFrame, flatConf: FlatTableConfig): Unit = {

    import DataFrameStatistics.Statistics

    logger.info(s"Computing Statistics on the flat table: ${flatConf.tableName}")
    println(flatConf)

    val flatTableStats = data
      .drop("year")
      .customDescribe()
      .withColumn("TableName", lit(flatConf.tableName))
      .persist()

    flatTableStats.write.parquet(flatConf.outputStatPath + "/flat_table")

    if(flatConf.singleTables.nonEmpty) {
      val singleTablesStats = flatConf.singleTables.map {
        singleTableConf =>
          val singleTableData = readParquet(data.sqlContext, singleTableConf.inputPath).drop("year")
          val isCentral = flatConf.centralTable == singleTableConf.tableName
          computeSingleTableStats(singleTableData, isCentral, singleTableConf, flatConf.joinKeys)
      }.reduce(_.union(_)).persist()

      singleTablesStats.write.parquet(flatConf.outputStatPath + "/single_tables")
      exceptOnColumns(
        flatTableStats.select(singleTablesStats.columns.map(col): _*),
        singleTablesStats,
        (singleTablesStats.columns.toSet - "TableName").toList
      ).write.parquet(flatConf.outputStatPath + "/diff")
    }
  }

  def exceptOnColumns(left: DataFrame, right: DataFrame, colNames: List[String]): DataFrame = {

    val window = Window.partitionBy(colNames.map(col): _*)
    left.union(right)
      .withColumn("count", count("*").over(window))
      .where(col("count") < 2)
      .drop("count")
  }

  override def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    import DataFrameHelper.ImplicitDF
    // Compute and save stats for the old flattening
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

    // Compute and save stats for the new flattening
    StatisticsConfig.mainFlatConfig.foreach { conf =>
      val flatData = readParquet(sqlContext, conf.inputPath)
      describeFlatTable(flatData, conf)
    }

    None
  }
}
