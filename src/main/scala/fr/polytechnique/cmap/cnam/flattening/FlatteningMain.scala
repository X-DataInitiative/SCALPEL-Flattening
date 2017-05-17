package fr.polytechnique.cmap.cnam.flattening

import com.sun.prism.PixelFormat.DataType
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.utilities.DFUtils
import org.joda.time.DateTime

object FlatteningMain extends Main {

  def appName = "Flattening"

  def saveCSVTablesAsParquet(sqlContext: SQLContext,
                             saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    // Generate schemas from csv
    val columnsTypeMap: Map[String, List[(String, String)]] = FlatteningConfig.columnTypes


    FlatteningConfig.partitionsList.foreach {
      config: ConfigPartition =>
        val columnsType = columnsTypeMap(config.name).toMap

        val rawTable = DFUtils.readCSV(sqlContext, config.inputPaths)
        val typedTable = DFUtils.applySchema(rawTable, columnsType, config.dateFormat)
        import org.apache.spark.sql.Column
        import org.apache.spark.sql.functions._
        val partitionMonthColumnName = config.monthPartitionColumn
//        if(partitionMonthColumnName != ""){
//          val partitionMonthColumn: Column = month(typedTable(partitionMonthColumnName))
//          typedTable.repartition(6,partitionMonthColumn).write.parquet(config.output)
//        }
//        else
          typedTable.write.parquet(config.output)

    }
  }

  def computeFlattenedFiles(sqlContext: SQLContext, configs: List[Config]): Unit = {
    configs.foreach(config => new FlatTable(sqlContext, config).writeAsParquet)
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    logger.info("begin converting csv to parquet")
    saveCSVTablesAsParquet(sqlContext)

    logger.info("begin flattening")
    computeFlattenedFiles(sqlContext, FlatteningConfig.joinTablesConfig)

    logger.info("finished flattening")

    None
  }
}
