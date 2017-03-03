package fr.polytechnique.cmap.cnam.flattening
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
import org.apache.spark.sql.{Dataset, SQLContext, SaveMode}
import fr.polytechnique.cmap.cnam.Main
import org.apache.spark.sql

/**
  * Created by sathiya on 15/02/17.
  */
object FlatteningMain extends Main {

  def appName = "Flattening"

  def saveCSVTablesAsParquet(saveMode: SaveMode = SaveMode.Overwrite): Unit = {

    import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
    import fr.polytechnique.cmap.cnam.flattening.ReadCSVTable._

    val schemaFile = readSchemaFile(sqlContext, FlatteningConfig.schemaFile)

    FlatteningConfig.tablesConfigList
      .foreach{
        table =>
          println("################################################################################")
          println(s"Preparing to WRITE CSV Table: ${table.name}, PartitionKey: ${table.partitionKey}")
          readCSVTable(sqlContext, table)
            .applySchema(schemaFile, table)
            .write
            .mode(saveMode)
            .parquet(FlatteningConfig.outputPath + "/" + table.name + "/key=" + table.partitionKey)
          println("##########################----------DONE---------###############################")
      }
  }

  def computeFlattenedFiles(sqlContext: SQLContext, argsMap: Map[String, String] = Map()): Unit = {

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))
    //logger.info("begin join")
    val flatTables = FlatteningConfig.joinTablesConfig
      .map(
        config => {
          config.name -> new FlattenedTable(config, sqlContext)
        }
      ).toMap
    //sqlContext.setConf("spark.sql.shuffle.partitions","2001")
    logger.info("begin save")
    flatTables.foreach(x => x._2.saveJoinTable())
    logger.info("end save join")
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    logger.info("begin run")
    argsMap("strategy" )match {
      case "convert" => saveCSVTablesAsParquet()
      case "join" => computeFlattenedFiles(sqlContext, argsMap)
    }

    None
  }
}

