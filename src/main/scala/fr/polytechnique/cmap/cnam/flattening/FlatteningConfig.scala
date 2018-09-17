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
  singleTablePath: String,
  schemaFilePath: List[String] = List.empty[String],
  join: List[JoinTableConfig] = List.empty[JoinTableConfig],
  tablesConfig: List[TablesConfig] = List.empty[TablesConfig]
) extends Config {

  private lazy val csvSchema = CSVSchemaReader.readSchemaFiles(schemaFilePath)

  lazy val columnTypes: Map[String, List[(String, String)]] = CSVSchemaReader.readColumnsType(csvSchema)

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

  def toConfigPartition(
    rootPath: String,
    tableConfig: TableConfig,
    partitionConfig: PartitionConfig): ConfigPartition = {
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
    inputPath: String,
    joinKeys: List[String] = List.empty[String],
    tablesToJoin: List[String] = List.empty[String],
    mainTableName: String,
    flatOutputPath: String,
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
