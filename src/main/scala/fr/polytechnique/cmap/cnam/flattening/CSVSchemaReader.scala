package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.utilities.CollectionTool._

/**
  * Created by sathiya on 15/02/17.
  */
object CSVSchemaReader {

  private val TABLE_NAME_INDEX = 0
  private val COLUMN_NAME_INDEX = 1
  private val COLUMN_TYPE_INDEX = 5

  def readSchemaFile(filename: String): List[String] = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(filename)
    scala.io.Source.fromInputStream(inputStream).getLines().toList
  }

  def readSchemaFiles(filenames: List[String]): List[String] = {
    filenames.map(readSchemaFile).reduce(_ ++ _)
  }

  def readColumnsType(configLines: List[String]): Map[String, List[(String,String)]] = {
    configLines
      .map(_.split(';'))
      .map{
        line => line(TABLE_NAME_INDEX) -> (line(COLUMN_NAME_INDEX), line(COLUMN_TYPE_INDEX))
      }.groupByKey()
  }


}
