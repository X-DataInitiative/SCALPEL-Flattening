package fr.polytechnique.cmap.cnam.statistics.descriptive

import fr.polytechnique.cmap.cnam.config.{Config, ConfigLoader}
import fr.polytechnique.cmap.cnam.flattening.CSVSchemaReader

case class StatisticsConfig(
  describeOld: Boolean = true,
  schemaFilePath: List[String] = List.empty[String],
  oldFlat: List[FlatTableConfig] = List.empty[FlatTableConfig],
  newFlat: List[FlatTableConfig] = List.empty[FlatTableConfig]
) extends Config {

  private lazy val csvSchema = CSVSchemaReader.readSchemaFiles(schemaFilePath)

  lazy val columnTypes: Map[String, List[(String, String)]] = CSVSchemaReader.readColumnsType(csvSchema)
}

object StatisticsConfig extends ConfigLoader {

  /**
    * Reads a configuration file and merges it with the default file.
    *
    * @param path The path of the given file.
    * @param env  The environment in the config file (usually can be "cmap", "cnam" or "test").
    * @return An instance of StatisticsConfig containing all parameters.
    */
  def load(path: String, env: String): StatisticsConfig = {
    val defaultPath = "statistics/main.conf"
    loadConfigWithDefaults[StatisticsConfig](path, defaultPath, env)
  }

}
