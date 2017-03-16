package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql._

import com.typesafe.config.Config

import fr.polytechnique.cmap.cnam.Main
import fr.polytechnique.cmap.cnam.utilities.DFUtils
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._

/**
  * Created by sathiya on 15/02/17.
  */
object FlatteningMain extends Main {

  def appName = "Flattening"

  def saveCSVTablesAsParquet(sqlContext: SQLContext,
                             saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    // Generate schemas from csv
    val columnsTypeMap: Map[String, List[(String,String)]] = FlatteningConfig.columnTypes


    FlatteningConfig.partitionsList.foreach{
      config: ConfigPartition =>
        val columnsType = columnsTypeMap(config.name).toMap

        val rawTable = DFUtils.readCSV(sqlContext, config.inputPaths)
        val typedTable = DFUtils.applySchema(rawTable, columnsType, config.dateFormat)

        typedTable.write.parquet(config.output)
    }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    saveCSVTablesAsParquet(sqlContext)
    None
  }
}
