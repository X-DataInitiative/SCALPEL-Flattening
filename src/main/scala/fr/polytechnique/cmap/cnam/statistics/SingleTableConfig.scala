package fr.polytechnique.cmap.cnam.statistics

import com.typesafe.config.Config

case class SingleTableConfig(
    tableName: String,
    dateFormat: String,
    inputPath: String) {

  override def toString: String = {
    s"tableName -> $tableName \n" +
    s"dateFormat -> $dateFormat \n" +
    s"inputPath -> $inputPath"
  }
}

object SingleTableConfig {

  def fromConfig(c: Config): SingleTableConfig = {
    SingleTableConfig(
      tableName = c.getString("table_name"),
      dateFormat = if (c.hasPath("date_format")) c.getString("date_format") else "dd/MM/yyyy",
      inputPath = c.getString("input_path")
    )
  }
}

