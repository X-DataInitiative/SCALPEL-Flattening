package fr.polytechnique.cmap.cnam.flattening

import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import com.typesafe.config.Config

/**
  * Created by sathiya on 15/02/17.
  */
object ReadCSVTable {

  implicit class FlatteningDFUtilities(data: DataFrame) {
    import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.FlatteningConfigUtilities

    val parseDate = udf(
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

    def applySchema(schemaFile: DataFrame, tableConfig: FlatteningConfigUtilities): DataFrame = {
      val expectedSchema = getSchema(schemaFile, tableConfig.name)

      val castedColNames = data.columns.map { colName =>
        if (expectedSchema.contains(colName)) {
          expectedSchema(colName) match {
            case "Date" => parseDate(col(colName), lit(tableConfig.dateFormat))
              .cast("Date").as(colName)
            case _ => col(colName).cast(expectedSchema(colName))
          }
        }
        else
          col(colName)
      }

      data.select(castedColNames: _*)
    }
  }

  def getSchema(schemaFile: DataFrame, tableName: String): Map[String, String] = {
    val tableSchema = schemaFile.filter(col("MEMNAME") like tableName)
      .select("NAME", "FORMAT", "FORMATL", "FORMATD").distinct

    val tableColumns = tableSchema.select("NAME").distinct

    if(tableSchema.count != tableColumns.count)
      throw new Exception(s"Conflicting Schema definition for the table $tableName")

    val computeDataType = udf(
      (format: String, formatL: Int, formatD: Int) => (format, formatL, formatD) match {
        case (null, formatL, 0) if formatL < 10 => "Integer"
        case (null, formatL, 0) if formatL >= 10 => "Long"
        case (null, formatL, formatD) if formatL < 6 && formatD > 0 => "Float"
        case (null, formatL, formatD) if formatL >= 6 && formatD > 0 => "Double"
        case ("$", formatL, 0) => "String"
        case ("DATETIME", formatL, 0) => "Date"
        case _ => "String"
      }
    )

    val tableSchemaWithDataType = tableSchema
      .withColumn("DATATYPE", computeDataType(col("FORMAT"), col("FORMATL"), col("FORMATD")))

    import tableSchemaWithDataType.sqlContext.implicits._
    tableSchemaWithDataType
      .map(
        (row: Row) =>
          row.getString(0) -> row.getString(4)
      ).collect().toMap
  }

  def readSchemaFile(sqlContext: SQLContext,
    inputPath: List[String]): DataFrame = readCSV(sqlContext, inputPath)

  def readCSVTable(sqlContext: SQLContext, tableConfig: Config): DataFrame = {

    import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig._
    readCSV(sqlContext, tableConfig.inputPaths, tableConfig.dateFormat)
  }

  def readCSV(sqlContext: SQLContext,
    inputPath: List[String],
    dateFormat: String = "dd/MM/yyyy"): DataFrame = {
    sqlContext
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(inputPath: _*)
  }
}
