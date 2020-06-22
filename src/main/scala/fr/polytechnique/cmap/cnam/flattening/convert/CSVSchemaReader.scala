package fr.polytechnique.cmap.cnam.flattening.convert

import fr.polytechnique.cmap.cnam.flattening.Schema
import fr.polytechnique.cmap.cnam.utilities.CollectionTool._

object CSVSchemaReader {

  final val delimiter: String = ";"
  final val tableCSVName: String = "MEMNAME"
  final val columnCSVName: String = "NAME"
  final val typeCSVName: String = "DATATYPE"

  def readSchemaFile(filename: String): List[String] = {
    val inputStream = getClass.getClassLoader.getResourceAsStream(filename)
    scala.io.Source.fromInputStream(inputStream).getLines().toList
  }

  def readSchemaFiles(filenames: List[String]): List[String] = {
    filenames.map(readSchemaFile).reduce(_ ++ _)
  }

  def readColumnsType(configLines: List[String]): Schema = {

    val colNameLookup: Map[String, Int] = configLines.head.split(delimiter).map(_.trim).zipWithIndex.toMap

    val tableNameIndex = colNameLookup(tableCSVName)
    val columnNameIndex = colNameLookup(columnCSVName)
    val columnTypeIndex = colNameLookup(typeCSVName)

    configLines
      .map(_.split(delimiter).map(_.trim))
      .map {
        line => line(tableNameIndex) -> (line(columnNameIndex), line(columnTypeIndex))
      }.groupByKey - tableCSVName
  }


}
