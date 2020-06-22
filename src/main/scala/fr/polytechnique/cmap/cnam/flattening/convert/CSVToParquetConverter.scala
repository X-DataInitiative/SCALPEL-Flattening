package fr.polytechnique.cmap.cnam.flattening.convert

import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.flattening.TableColumnsActions._
import fr.polytechnique.cmap.cnam.flattening.tables.{SingleTable, Table}
import fr.polytechnique.cmap.cnam.flattening.{ConfigPartition, Schema}
import fr.polytechnique.cmap.cnam.utilities.DFUtils._

object CSVToParquetConverter extends Converter[SingleTable] {

  override def read(sqlContext: SQLContext, config: ConfigPartition, schema: Schema): Table[SingleTable] =
    SingleTable(config.name, readCSV(sqlContext, config.inputPaths).applySchema(schema(config.name).toMap, config.dateFormat).processActions(config))


  override def write(table: Table[SingleTable], config: ConfigPartition): Unit = {
    //Do not partition data with a column including only few values
    //it will cause data skew and reduce the performance when huge data comes
    if (!config.partitionColumn.isDefined) {
      table.df.writeParquet(config.output)(config.singleTableSaveMode)
    } else {
      table.df.writeParquet(config.output, config.partitionColumn.get)(config.singleTableSaveMode)
    }
  }
}
