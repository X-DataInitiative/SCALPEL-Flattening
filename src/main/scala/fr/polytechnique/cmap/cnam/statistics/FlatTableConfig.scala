package fr.polytechnique.cmap.cnam.statistics

import scala.collection.JavaConverters._
import com.typesafe.config.Config
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig

case class FlatTableConfig(
    tableName: String,
    centralTable: String,
    joinKeys: List[String],
    dateFormat: String,
    inputPath: String,
    outputStatPath: String,
    singleTables: List[SingleTableConfig]) {

  override def toString: String = {
    s"tableName -> $tableName \n" +
    s"centralTable -> $centralTable \n" +
    s"joinKeys -> $joinKeys \n" +
    s"dateFormat -> $dateFormat \n" +
    s"inputPath -> $inputPath \n" +
    s"outputStatPath -> $outputStatPath \n" +
    s"singleTableCount -> ${singleTables.size}"
  }
}

object FlatTableConfig {

  def fromConfig(c: Config): FlatTableConfig = {

    val singleTables = if (c.hasPath("single_tables"))
      c.getConfigList("single_tables").asScala.toList.map(SingleTableConfig.fromConfig)
    else
      List[SingleTableConfig]()

    import FlatteningConfig.JoinConfig
    // The next line gets the join keys from the Flattening Config. This is temporary until the
    // stats config is merged with the flattening config.
    val joinKeys: List[String] = FlatteningConfig.joinTablesConfig.find { conf =>
      conf.nameFlatTable == c.getString("name")
    }.fold(List[String]())(_.foreignKeys)
    // Applying fold to an Option:
    // "Returns the result of applying f to this scala.Option's value if the scala.Option is nonempty."

    FlatTableConfig(
      tableName = c.getString("name"),
      centralTable = c.getString("central_table"),
      joinKeys = joinKeys,
      dateFormat = if (c.hasPath("date_format")) c.getString("date_format") else "dd/MM/yyyy",
      inputPath = c.getString("input_path"),
      outputStatPath = c.getString("output_stat_path"),
      singleTables = singleTables
    )
  }
}
