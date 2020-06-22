package fr.polytechnique.cmap.cnam.flattening.convert

import org.apache.spark.sql.SQLContext
import fr.polytechnique.cmap.cnam.flattening.tables.{AnyTable, Table}
import fr.polytechnique.cmap.cnam.flattening.{ConfigPartition, Schema}

trait Converter[A <: AnyTable] {

  def read(sqlContext: SQLContext, config: ConfigPartition, schema: Schema): Table[A]

  def write(table: Table[A], config: ConfigPartition): Unit

  def convert(sqlContext: SQLContext, config: ConfigPartition, schema: Schema): Unit = write(read(sqlContext, config, schema), config)

}
