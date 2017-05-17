package fr.polytechnique.cmap.cnam.flattening

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

case class ConfigPartition(
    name: String,
    dateFormat: String,
    inputPaths: List[String],
    output: String,
    monthPartitionColumn: String)

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

  val outputBasePath: String = conf.getString("single_table_path")

  val tablesConfigList: List[Config] = conf.getConfigList("tables_config").asScala.toList
    .map(_.getConfigList("tables").asScala.toList)
    .reduce(_ ::: _)

  val partitionsList: List[ConfigPartition] = getPartitionList(tablesConfigList)
  val joinTablesConfig: List[Config] = conf.getConfigList("join").asScala.toList

  private val csvSchema = CSVSchemaReader.readSchemaFiles(schemaFilePath)
  val columnTypes: Map[String, List[(String, String)]] = CSVSchemaReader.readColumnsType(csvSchema)

  implicit class SingleTableConfig(config: Config) {

    def name: String = config.getString("name")

    def strategy: String = config.getString("partition_strategy")

    def dateFormat: String = {
      if (config.hasPath("date_format"))
        config.getString("date_format")
      else "dd/MM/yyyy"
    }
    def monthPartitionColumn: String = config.getString("month_partition_column")

    def partitions: List[Config] = config.getConfigList("partitions").asScala.toList

    def inputPaths: List[String] = config.getStringList("path").asScala.toList

  }

  implicit class SinglePartitionConfig(config: Config) {

    def outputPath(strategy: String, name: String): String = {
      if (strategy == "year")
        outputBasePath + "/" + name + "/year=" + config.getString("year")
      else
        outputBasePath + "/" + name
    }
  }

  implicit class JoinConfig(config: Config) {

    def nameFlatTable: String = config.getString("name")

    def inputPath: String = if (config.hasPath("input_path")) config.getString("input_path")
    else outputBasePath

    def tablesToJoin: List[String] = config.getStringList("tables_to_join").asScala.toList

    def foreignKeys: List[String] = config.getStringList("join_keys").asScala.toList

    def mainTableName: String = config.getString("main_table_name")

    def outputJoinPath: String = config.getString("flat_output_path")
  }

  def getPartitionList(tableConfigs: List[Config]): List[ConfigPartition] = {
    tableConfigs.flatMap {
      tableConfig =>
        tableConfig.partitions.map(toConfigPartition(tableConfig, _))
    }
  }

  def toConfigPartition(tableConfig: Config, partitionConfig: Config): ConfigPartition = {
    ConfigPartition(
      name = tableConfig.name,
      dateFormat = tableConfig.dateFormat,
      inputPaths = partitionConfig.inputPaths,
      output = partitionConfig.outputPath(tableConfig.strategy, tableConfig.name),
      monthPartitionColumn = tableConfig.monthPartitionColumn
    )
  }


}
