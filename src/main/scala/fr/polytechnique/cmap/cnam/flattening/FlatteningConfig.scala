package fr.polytechnique.cmap.cnam.flattening

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession

import com.typesafe.config.{Config, ConfigFactory}

import fr.polytechnique.cmap.cnam.utilities.CollectionTool._

case class ConfigPartition(
                          name: String,
                          dateFormat: String,
                          inputPaths: List[String],
                          output: String
                          )

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

  val schemaFilePath: List[String] = conf.getStringList("schema_file_path").asScala.toList

  val outputBasePath: String = conf.getString("output_path")

  val tablesConfigList: List[Config] = conf.getConfigList("tables_config").asScala.toList
    .map(_.getConfigList("tables").asScala.toList)
    .reduce(_ ::: _)

  val partitionsList: List[ConfigPartition] = getPartitionList(tablesConfigList)

  private val csvSchema = CSVSchemaReader.readSchemaFiles(schemaFilePath)
  val columnTypes: Map[String, List[(String,String)]] = CSVSchemaReader.readColumnsType(csvSchema)

  implicit class SingleTableConfig(config: Config) {

    def name: String = config.getString("name")
    def strategy: String = config.getString("partition_strategy")

    def dateFormat: String = {
      if(config.hasPath("date_format"))
        config.getString("date_format")
      else "dd/MM/yyyy"
    }

    def partitions: List[Config] = config.getConfigList("partitions").asScala.toList

    def inputPaths: List[String] = config.getStringList("path").asScala.toList

    def partitionColumn: Option[String] = {
      if(config.hasPath("output.partition_column"))
        Some(config.getString("output.partition_column"))
      else None
    }
  }

  implicit class SinglePartitionConfig(config: Config) {

    def outputPath(strategy: String, name: String): String = {
      if(strategy == "year")
        outputBasePath + "/" + name + "/year=" + config.getString("year")
      else
        outputBasePath + "/" + name
    }
  }

  def getPartitionList(tableConfigs: List[Config]): List[ConfigPartition] = {
    tableConfigs.flatMap{
      config =>
        config.partitions.map{
          partitionConfig: Config =>
            ConfigPartition(
              name = config.name,
              dateFormat = config.dateFormat,
              inputPaths = partitionConfig.inputPaths,
              output = partitionConfig.outputPath(config.strategy, config.name)
            )
        }
    }
  }


}
