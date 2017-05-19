package fr.polytechnique.cmap.cnam.statistics

import org.apache.spark.sql.{Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig
import fr.polytechnique.cmap.cnam.utilities.DFUtils.readParquet
import fr.polytechnique.cmap.cnam.{Main, utilities}

case class TableSchema(tableName: String, columnTypes: Map[String, String])

object StatisticsMain extends Main {

  override def appName = "Statistics"

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {

    def describeOldFlatTable(): Unit = {

      import FlatTableHelper.ImplicitDF
      import StatisticsConfig.StatConfig

      val tablesSchema: List[TableSchema] = FlatteningConfig.columnTypes.map(x =>
        TableSchema(x._1, x._2.toMap)).toList

      StatisticsConfig.oldFlatConfig.foreach { conf =>
        logger.info(s"Computing Statistics on the old Flat: ${conf.flatTableName}")
        println(conf.prettyPrint)

        readParquet(sqlContext, conf.inputPath)
          .drop("key")
          .changeColumnNameDelimiter
          .changeSchema(tablesSchema, conf.mainTableName, conf.dateFormat)
      }
    }

    def describeSingleTables(): Unit = {

      import StatisticsConfig.StatConfig

      StatisticsConfig.singleTablesConfig.foreach { conf =>
        logger.info(s"Computing Statistics on the single table: ${conf.flatTableName}")
        println(conf.prettyPrint)

        import CustomStatistics.Statistics
        readParquet(sqlContext, conf.inputPath)
          .customDescribe(distinctOnly = true)
          .write.parquet(conf.statOutputPath)
      }
    }

    def describeMainFlatTable(): Unit = {

      import FlatTableHelper.ImplicitDF
      import StatisticsConfig.StatConfig

      StatisticsConfig.mainFlatConfig.foreach { conf =>
        logger.info(s"Computing Statistics on the main Flat: ${conf.flatTableName}")
        println(conf.prettyPrint)

        readParquet(sqlContext, conf.inputPath)
          .drop("year")
          .writeStatistics(conf.statOutputPath)
      }
    }

    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    if(StatisticsConfig.compareWithOldFlattening) describeOldFlatTable()
    describeSingleTables()
    describeMainFlatTable()

    None
  }
}
