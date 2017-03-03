package fr.polytechnique.cmap.cnam.flattening

import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.FlatteningTableConfig

/**
  * Created by sathiya on 15/02/17.
  */
object ReadCSVTable {

  implicit class FlatteningDFUtilities(data: DataFrame) {

    val parseDate: UserDefinedFunction = udf(
      (dateInString: String, format: String) => {
        if(dateInString == null)
          null
        else
          {
            val dateFormatter = new SimpleDateFormat(format)
            new Date(dateFormatter.parse(dateInString).getTime)
          }
      }
    )

    def applySchema(schemaFile: List[String], tableConfig: FlatteningTableConfig): DataFrame = {
      val expectedSchema = getSchema(schemaFile, tableConfig.schemaId)

      val castedColNames = data.columns.map { colName =>
        if (expectedSchema.contains(colName)) {
          expectedSchema(colName) match {
            case "Date" => parseDate(col(colName), lit(tableConfig.dateFormat))
              .cast("Date").as(colName)
            case _ => col(colName).cast(expectedSchema(colName))
          }
        }
        else {
          val logger = Logger.getLogger(getClass)
          logger.error(s"Column name: $colName for schema id: ${tableConfig.schemaId} not found, " +
            s"choosing StringType")
          col(colName)
        }
      }

      data.select(castedColNames: _*)
    }
  }

  def getSchema(schemaLines: List[String], tableName: String): Map[String, String] = {

    val tableSchema: List[(String, String)] = schemaLines
      .map( _.split(";").map(_.trim))
      .filter(_.apply(0).equals(tableName))
      .map(
        (row: Array[String]) =>
          (row.apply(1), row.apply(5))
      )

    val tableColumns = tableSchema.map(_._1).distinct

    if(tableSchema.size != tableColumns.size)
      throw new Exception(s"Conflicting Schema definition for the table $tableName")

    tableSchema.toMap
  }

  def readSchemaFile(schemaPath: List[String]): List[String] = schemaPath
    .map(scala.io.Source.fromFile(_).getLines)
    .reduce(_ ++ _).toList

  def readCSVTable(sqlContext: SQLContext, tableConfig: FlatteningTableConfig): DataFrame = {
    readCSV(sqlContext, tableConfig.inputPaths, tableConfig.dateFormat)
  }

  def readCSV(sqlContext: SQLContext,
    inputPath: Seq[String],
    dateFormat: String = "dd/MM/yyyy"): DataFrame = {
    sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(inputPath: _*)
  }
}
