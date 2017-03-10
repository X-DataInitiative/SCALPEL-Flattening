package fr.polytechnique.cmap.cnam.flattening

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by sathiya on 15/02/17.
  */
object FlatteningConfig {

  private lazy val conf: Config = {
    //TODO: Find a cleaner way to do the same
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")

    val defaultConfig = ConfigFactory.parseResources("flattening/config/main.conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()

    newConfig.withFallback(defaultConfig).resolve()
  }

  val envName: String = conf.getString("env_name")
  val schemaFilePath: List[String] = conf.getStringList("schema_file_path").asScala.toList
  val outputPath: String = conf.getString("output_path")

  val tablesConfigList : List[Config] = conf.getConfigList("tables_config").asScala.toList
    .map(_.getConfigList("tables").asScala.toList)
    .reduce(_ ::: _)

  implicit class FlatteningTableConfig(config: Config) {

    def name: String = config.getString("name")

    def schemaId: String = config.getString("schema_id")

    def dateFormat: String = {
      if(config.hasPath("date_format"))
        config.getString("date_format")
      else "dd/MM/yyyy"
    }

    def inputPaths: List[String] = config.getStringList("input").asScala.toList

    def outputTableName: String = config.getString("output.table_name")

    def partitionColumn: Option[String] = {
      if(config.hasPath("output.partition_column"))
        Some(config.getString("output.partition_column"))
      else None
    }

    override def toString: String = {
      s"name -> $name \n" +
      s"schemaName -> $schemaId \n" +
      s"dateFormat -> $dateFormat \n" +
      s"inputPaths -> $inputPaths \n" +
      s"outputTableName -> $outputTableName \n" +
      s"partitionColumn -> $partitionColumn"
    }
  }

  def tableConfig(name: String): Config = {
    tablesConfigList.filter(_.name == name).head
  }

  def tablesConfigOutputPath: List[String] = tablesConfigList.map(_.outputTableName)

  def tablesConfigNames: List[String] = tablesConfigList.map(_.name)

  def tablesConfigSchemaIds: List[String] = tablesConfigList.map(_.schemaId)

  override def toString: String = {
    s"envName -> $envName \n" +
    s"schemaFilePath -> $schemaFilePath \n" +
    s"outputPath -> $outputPath \n" +
    s"tablesConfigList -> $tablesConfigNames"
  }
}
