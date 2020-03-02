// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.config.{Config, ConfigLoader}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.{JoinTableConfig, TableConfig, TablesConfig, toConfigPartition}

case class ConfigPartition(
  name: String,
  dateFormat: String,
  inputPaths: List[String],
  output: String,
  partitionColumn: Option[String] = None,
  saveSingleTable: Boolean = true,
  singleTableSaveMode: String = "append",
  actions: List[String] = List.empty[String])

case class FlatteningConfig(
  basePath: String,
  delimiter: String = ";",
  withTimestamp: Boolean = false,
  timestampFormat: String = "/yyyy_MM_dd",
  autoBroadcastJoinThreshold: Option[String] = None,
  schemaFilePath: List[String] = List.empty[String],
  join: List[JoinTableConfig] = List.empty[JoinTableConfig],
  tablesConfig: List[TablesConfig] = List.empty[TablesConfig]
) extends Config {

  import fr.polytechnique.cmap.cnam.utilities.DFUtils.StringPath

  private lazy val csvSchema = CSVSchemaReader.readSchemaFiles(schemaFilePath)

  private lazy val root = if (withTimestamp) basePath.withTimestampSuffix(format = timestampFormat) else basePath

  lazy val columnTypes: Map[String, List[(String, String)]] = CSVSchemaReader.readColumnsType(csvSchema)

  lazy val singleTablePath: String = root + "/single_table"

  lazy val flatTablePath: String = root + "/flat_table"

  lazy val joinTableConfigs: List[JoinTableConfig] = join.map(config => {
    val input = if (config.inputPath.isDefined) config.inputPath else Some(singleTablePath)
    val output = if (config.flatOutputPath.isDefined) config.flatOutputPath else Some(flatTablePath)
    val refs = config.refsToJoin.map {
      refConfig =>
        val refInput = if (refConfig.inputPath.isDefined) refConfig.inputPath else input
        refConfig.copy(inputPath = refInput)
    }
    config.copy(inputPath = input, flatOutputPath = output, refsToJoin = refs)
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

  def toConfigPartition(
    rootPath: String,
    tableConfig: TableConfig,
    partitionConfig: PartitionConfig): ConfigPartition = {
    ConfigPartition(
      name = tableConfig.name,
      dateFormat = tableConfig.dateFormat,
      inputPaths = partitionConfig.path,
      output = partitionConfig.outputPath(rootPath, tableConfig.partitionStrategy, tableConfig.name),
      partitionColumn = tableConfig.partitionColumn,
      saveSingleTable = partitionConfig.saveSingleTable,
      singleTableSaveMode = partitionConfig.singleTableSaveMode,
      actions = tableConfig.actions)
  }

  case class YearAndMonths(year: Int, months: List[Int] = List.empty[Int])

  case class Reference(
    name: String,
    inputPath: Option[String] = None,
    //inner list represents the key pairs used to join flat table and reference
    joinKeys: List[List[String]] = List.empty[List[String]]
  ) {
    lazy val joinKeysTuples: List[(String, String)] = joinKeys.filter(_.size == 2).map {
      case a :: b :: Nil => if (a.startsWith(name)) (b, a) else (a, b)
    }
  }

  case class JoinTableConfig(
    name: String,
    inputPath: Option[String] = None,
    joinKeys: List[String] = List.empty[String],
    joinKeysPatient: Option[List[String]] = None,
    tablesToJoin: List[String] = List.empty[String],
    mainTableName: String,
    pmsiPatientTableName: Option[String] = None,
    flatOutputPath: Option[String] = None,
    monthlyPartitionColumn: Option[String] = None,
    saveFlatTable: Boolean = true,
    flatTableSaveMode: String = "append",
    onlyOutput: List[YearAndMonths] = List.empty[YearAndMonths],
    refsToJoin: List[Reference] = List.empty[Reference])

  case class PartitionConfig(
    year: Option[String] = None,
    path: List[String] = List.empty[String],
    saveSingleTable: Boolean = true,
    singleTableSaveMode: String = "append") {

    /*
     * value table transforms to parquet files with the same table name
     * normal table transforms to parquet files with table name and year
     */
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
    partitionColumn: Option[String] = None,
    actions: List[String] = List.empty[String] //actions applied to the table. For example addMoleculeCombinationColumn to IR_PHA_R
  )

  case class TablesConfig(tables: List[TableConfig])

}
