package fr.polytechnique.cmap.cnam.flattening

import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConversions._
import com.typesafe.config._
/**
  * Created by sathiya on 15/02/17.
  */
object FlatteningConfig {

  private lazy val conf: Config = {
    //TODO: Find a cleaner way to do the same
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")

    val defaultConfig = ConfigFactory.parseResources("flattening/config/pmsi.conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()

    newConfig.withFallback(defaultConfig).resolve()
  }

  val schemaFile: List[String] = conf.getStringList("schema_file_path").asScala.toList
  val outputPath: String = conf.getString("output_path")

  val tablesConfigList : List[Config] = conf.getConfigList("tables_config").asScala.toList

  val joinTablesConfig : List[Config] = conf.getConfigList("join").asScala.toList




  implicit class FlatteningConfigUtilities(config: Config) {

    //reading implicits
    def name: String = config.getString("name")

    def schemaName: String = config.getString("schema_name")

    def dateFormat: String = config.getString("date_format")

    def inputPaths: List[String] = config.getStringList("input_path").asScala.toList

    def partitionKey: List[String] = config.getStringList("output.key").asScala.toList

    //joining implicits
    def tablesToJoin: List[Config] = config.getConfigList("tablesToJoin").asScala.toList
    def foreignKeys: List[String] = config.getStringList("foreignKeys").asScala.toList
    def pathTablesToJoin: String = config.getString("path")

    def mainTableName: String = config.getString("main_table.name")
    def mainTablePath: String = config.getString("main_table.path")
    def mainTableKey:  List[String]  = config.getStringList("main_table.primaryKey").asScala.toList
    def outputPath: String = config.getString("outputPath")

    def yearPartitionCols: List[String] =  config.getStringList("yearPartitionCols").asScala.toList
    def monthsPartitionCols: List[String] =  config.getStringList("monthsPartitionCols").asScala.toList

  }

  def getTableConfig(name: String): Config = {
    tablesConfigList.filter(_.name == name).head
  }
}
