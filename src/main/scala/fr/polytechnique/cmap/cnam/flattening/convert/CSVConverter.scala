package fr.polytechnique.cmap.cnam.flattening.convert

import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.flattening.TableColumnsActions._
import fr.polytechnique.cmap.cnam.flattening.tables.{SingleTable, Table}
import fr.polytechnique.cmap.cnam.flattening.{ConfigPartition, Schema}
import fr.polytechnique.cmap.cnam.utilities.DFUtils._

object CSVConverter extends Converter[SingleTable] {

  override def read(sqlContext: SQLContext, config: ConfigPartition, schema: Schema, format: String = "parquet"): Table[SingleTable] =
  // TODO: spark 3 support merge schema in ORC
    if (format == "orc" && getSparkVersion(sqlContext) < 3)
      SingleTable(config.name, readCSV(sqlContext, config.inputPaths).mergeSchema(schema(config.name).toMap).applySchema(schema(config.name).toMap, config.dateFormat).processActions(config))
    else
      SingleTable(config.name, readCSV(sqlContext, config.inputPaths).applySchema(schema(config.name).toMap, config.dateFormat).processActions(config))

  override def write(table: Table[SingleTable], config: ConfigPartition, format: String = "parquet"): Unit = {
    //Do not partition data with a column including only few values
    //it will cause data skew and reduce the performance when huge data comes
    if (!config.partitionColumn.isDefined) {
      table.df.write(config.output)(config.singleTableSaveMode, format)
    } else {
      table.df.write(config.output, config.partitionColumn.get)(config.singleTableSaveMode, format)
    }
  }

  private def getSparkVersion(sqlContext: SQLContext): Int = sqlContext.sparkSession.version.split("\\.").head.toInt

}
