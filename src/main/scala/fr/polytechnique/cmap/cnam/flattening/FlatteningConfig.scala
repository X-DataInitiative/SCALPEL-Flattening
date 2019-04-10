package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.config.{Config, ConfigLoader}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.{JoinTableConfig, TableConfig, TablesConfig, toConfigPartition}

case class ConfigPartition(
  name: String,
  dateFormat: String,
  inputPaths: List[String],
  output: String,
  partitionColumn: Option[String] = None)

case class FlatteningConfig(
  basePath: String,
  singleTableSaveMode: String = "append",
  schemaFilePath: List[String] = List.empty[String],
  join: List[JoinTableConfig] = List.empty[JoinTableConfig],
  tablesConfig: List[TablesConfig] = List.empty[TablesConfig]
) extends Config {

  import fr.polytechnique.cmap.cnam.utilities.DFUtils.StringPath

  private lazy val csvSchema = CSVSchemaReader.readSchemaFiles(schemaFilePath)

  private lazy val root = if (singleTableSaveMode == "withTimestamp") basePath.withTimestampSuffix() else basePath

  lazy val columnTypes: Map[String, List[(String, String)]] = CSVSchemaReader.readColumnsType(csvSchema)

  lazy val singleTablePath: String = root + "/single_table"

  lazy val flatTablePath: String = root + "/flat_table"

  lazy val joinTableConfigs: Seq[JoinTableConfig] = join.map(config => {
    val input = if (config.inputPath.isDefined) config.inputPath.get else singleTablePath
    val output = if (config.flatOutputPath.isDefined) config.flatOutputPath.get else flatTablePath
    config.copy(inputPath = Some(input), flatOutputPath = Some(output))
  })


  def partitions: List[ConfigPartition] = {
    for (table <- tables;
         partition <- table.partitions)
      yield toConfigPartition(singleTablePath, table, partition)
  }

  def tables: List[TableConfig] = {
    for (
      allTables <- tablesConfig;
      table <- allTables.tables)
      yield table
  }

}

object FlatteningConfig extends ConfigLoader {

  /**
    * Reads a configuration file and merges it with the default file.
    *
    * @param path The path of the given file.
    * @param env  The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of FlatteningConfig containing all parameters.
    */
  def load(path: String, env: String): FlatteningConfig = {
    val defaultPath = "flattening/config/main.conf"
    loadConfigWithDefaults[FlatteningConfig](path, defaultPath, env)
  }

  def toConfigPartition(rootPath: String, tableConfig: TableConfig, partitionConfig: PartitionConfig): ConfigPartition = {
    ConfigPartition(
      name = tableConfig.name,
      dateFormat = tableConfig.dateFormat,
      inputPaths = partitionConfig.path,
      output = partitionConfig.outputPath(rootPath, tableConfig.partitionStrategy, tableConfig.name),
      partitionColumn = tableConfig.partitionColumn
    )
  }

  case class JoinTableConfig(
    name: String,
    inputPath: Option[String] = None,
    joinKeys: List[String] = List.empty[String],
    tablesToJoin: List[String] = List.empty[String],
    mainTableName: String,
    flatOutputPath: Option[String] = None,
    monthlyPartitionColumn: Option[String] = None)

  case class PartitionConfig(
    year: Option[String] = None,
    path: List[String] = List.empty[String]) {

    def outputPath(root: String, strategy: String, name: String): String = {
      if (strategy == "year")
        root + "/" + name + "/year=" + year.get
      else
        root + "/" + name
    }

  }

  case class TableConfig(
    name: String,
    partitionStrategy: String,
    dateFormat: String = "dd/MM/yyyy",
    partitions: List[PartitionConfig] = List.empty[PartitionConfig],
    partitionColumn: Option[String] = None
  )

  case class TablesConfig(tables: List[TableConfig])

}
