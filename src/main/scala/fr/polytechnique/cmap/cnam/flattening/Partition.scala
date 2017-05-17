package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._


class Partition (data: DataFrame,
                 outputPath: String,
                 strategy: PartitionStrategy.Value = PartitionStrategy.None) {

  if (strategy == PartitionStrategy.Month) {

  }

  def write(): Unit = {
    if (strategy == PartitionStrategy.Month) {
      writeMonth()
    }else {
      simpleWrite()
    }
  }

  def simpleWrite(): Unit = {
    data.write.parquet(outputPath)
  }

  def writeMonth(): Unit = {
    val monthColumn = month(col("FLX_TRT_DTD"))
    val months = data.select(monthColumn)
      .distinct
      .collect()
      .toList
      .map(_(0).asInstanceOf[Int].toString)
    months.map {
      month =>
        data.filter(monthColumn === month).write.parquet(outputPath + "/month=" + month)
    }

  }
}
