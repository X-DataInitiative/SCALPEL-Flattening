package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.functions.{col, udf, lit}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import fr.polytechnique.cmap.cnam.Main

/**
  * Created by sathiya on 02/03/17.
  */

case class ComputeSchema(
  MEMNAME: String,
  NAME: String,
  FORMAT: String,
  FORMATL: String,
  FORMATD: String,
  DATATYPE: String
)

object SchemaColumn {
  val MEMNAME = 0
  val NAME = 1
  val FORMAT = 2
  val FORMATL = 3
  val FORMATD = 4
  val DATATYPE = 5
}

object ComputeSchema extends Main {

  def appName = "GenerateSchemaFile"

  val DbTablesMap: Map[String, Seq[String]] = ComputeSchemaConfig.databaseTablesMap
  val RawSchemaDefPaths: List[String] = ComputeSchemaConfig.rawSchemaFilePath
  val ComputedSchemaPath: String = ComputeSchemaConfig.outputPath

  def computeTableSchemas(sqlContext: SQLContext) = {
    val rawSchemaDefinition: DataFrame = readSchemaFilesAsDF(sqlContext, RawSchemaDefPaths)

    import sqlContext.implicits._
    DbTablesMap
      .foreach(
        (dataBaseTables: (String, Seq[String])) =>
          dataBaseTables._2
            .map(computeSchema(rawSchemaDefinition, _))
            .reduce(_.union(_))
            .coalesce(1)
            .as[ComputeSchema]
            .write
            .option("delimiter", ";")
            .option("header", "true")
            .csv(ComputedSchemaPath + "/" + dataBaseTables._1)
      )
  }

  import fr.polytechnique.cmap.cnam.flattening.ReadCSVTable._

  def readSchemaFilesAsDF(sqlContext: SQLContext,
    inputPath: Seq[String]): DataFrame = readCSV(sqlContext, inputPath)

  def computeSchema(rawSchemaDF: DataFrame, tableName: String): DataFrame = {
    val tableColFormat = rawSchemaDF.filter(col("MEMNAME") like tableName)
      .select("MEMNAME", "NAME", "FORMAT", "FORMATL", "FORMATD").distinct

    val computeDataType = udf(
      (name: String, format: String, formatL: Int, formatD: Int) =>
        (format, formatL, formatD) match {
            case (null, formatL, 0) if formatL < 10 => "Integer"
            case (null, formatL, 0) if formatL >= 10 => "Long"
            case (null, formatL, formatD) if formatL < 6 && formatD > 0 => "Float"
            case (null, formatL, formatD) if formatL >= 6 && formatD > 0 => "Double"
            case ("$", formatL, 0) => "String"
            case ("DATETIME", formatL, 0) => "Date"

            case _ => throw new Exception(s"Unknown column format exception for Column name: $name, " +
                                          s"in table: $tableName")
      }
    )

    tableColFormat
        .withColumn("DATATYPE", computeDataType(col("NAME"), col("FORMAT"), col("FORMATL"), col("FORMATD")))
        .withColumn("MEMNAME", lit(tableName))
        .distinct
  }

  def run(sqlContext: SQLContext, argsMap: Map[String, String]): Option[Dataset[_]] = {
    argsMap.get("conf").foreach(sqlContext.setConf("conf", _))
    argsMap.get("env").foreach(sqlContext.setConf("env", _))

    computeTableSchemas(sqlContext)
    None
  }

}
