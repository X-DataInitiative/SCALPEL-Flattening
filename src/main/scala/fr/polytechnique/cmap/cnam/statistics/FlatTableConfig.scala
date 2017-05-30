package fr.polytechnique.cmap.cnam.statistics

import scala.collection.JavaConverters._
import com.typesafe.config.Config

case class FlatTableConfig(
    tableName: String,
    centralTable: String,
    dateFormat: String,
    inputPath: String,
    outputStatPath: String,
    singleTables: List[SingleTableConfig]) {

  override def toString: String = {
    s"tableName -> $tableName \n" +
    s"centralTable -> $centralTable \n" +
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

    FlatTableConfig(
      tableName = c.getString("name"),
      centralTable = c.getString("central_table"),
      dateFormat = if (c.hasPath("date_format")) c.getString("date_format") else "dd/MM/yyyy",
      inputPath = c.getString("input_path"),
      outputStatPath = c.getString("output_stat_path"),
      singleTables = singleTables
    )
  }
}
