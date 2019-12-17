// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.utilities.CollectionTool._

object CSVSchemaReader {

  final val delimiter: String = ";"
  final val tableCSVName: String = "MEMNAME"
  final val columnCSVName: String = "NAME"
  final val typeCSVName: String = "DATATYPE"

  def readSchemaFile(filename: String): List[String] = {
    print(filename+'\n')
    /* Distinguish between in-jar schema put at "schema/..." and local csv to be read at execution time*/
    if (filename.split("/")(0) == "schema") {
      val inputStream = getClass.getClassLoader.getResourceAsStream(filename)
      scala.io.Source.fromInputStream(inputStream).getLines().toList
    }
    else {
      val csv_content = scala.io.Source.fromFile(filename)
      val csv_as_list = csv_content.getLines().toList
      csv_content.close()
      csv_as_list
    }
  }

  def readSchemaFiles(filenames: List[String]): List[String] = {
    filenames.map(readSchemaFile).reduce(_ ++ _)
  }

  def readColumnsType(configLines: List[String]): Map[String, List[(String, String)]] = {

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
