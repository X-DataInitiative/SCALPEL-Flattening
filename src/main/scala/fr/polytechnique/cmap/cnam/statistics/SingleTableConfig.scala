package fr.polytechnique.cmap.cnam.statistics

import com.typesafe.config.Config

case class SingleTableConfig(
    tableName: String,
    inputPath: String) {

  override def toString: String = {
    s"tableName -> $tableName \n" +
    s"inputPath -> $inputPath"
  }
}

object SingleTableConfig {

  def fromConfig(c: Config): SingleTableConfig = {
    SingleTableConfig(
      tableName = c.getString("name"),
      inputPath = c.getString("input_path")
    )
  }
}

