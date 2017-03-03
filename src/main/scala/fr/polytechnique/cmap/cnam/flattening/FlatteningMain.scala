package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
import fr.polytechnique.cmap.cnam.Main

/**
  * Created by sathiya on 15/02/17.
  */
object FlatteningMain extends Main {

  def appName = "Flattening"

  def saveCSVTablesAsParquet(sqlContext: SQLContext,
                             saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
    import fr.polytechnique.cmap.cnam.flattening.ReadCSVTable._

    val schemaFile: List[String] = readSchemaFile(FlatteningConfig.schemaFile)

    FlatteningConfig.tablesConfigList
      .foreach{
        table =>
          println("################################################################################")
          println(s"Preparing to WRITE CSV Table: ${table.outputTableName}, PartitionKey: ${table.partitionColumn}")
          readCSVTable(sqlContext, table)
            .applySchema(schemaFile, table)
            .write
            .mode(saveMode)
            .parquet(FlatteningConfig.outputPath + "/" + table.outputTableName)
          println("##########################----------DONE---------###############################")
      }
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    saveCSVTablesAsParquet(sqlContext)
    None
  }
}
