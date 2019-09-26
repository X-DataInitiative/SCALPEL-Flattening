package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SpecialFlatteningActions {

  implicit class SpecialActionDataFrame(dataFrame: DataFrame) {

    private[flattening] def addMoleculeCombinationColumn(): DataFrame = {
      val df = dataFrame.filter(col("PHA_NOM_PA").isNotNull)
      val pha = dataFrame.select("PHA_CIP_C13", "PHA_NOM_PA")

      val splitCol = explode(split(upper(col("PHA_NOM_PA")), ",|\\+"))
      val sortedCol = concat_ws("_",
        sort_array(collect_list(regexp_replace(col("molecule_combination"), " ", ""))))
      val moleculeCombinationCol = regexp_replace(sortedCol, "[\\(,\\),\\{,\\}]", "")
      val phaWithMol = pha.withColumn("molecule_combination", splitCol)
        .groupBy("PHA_CIP_C13").agg(moleculeCombinationCol.alias("molecule_combination"))


      df.join(phaWithMol, Seq("PHA_CIP_C13"), "left")

    }

    def processSpecialAction(config: ConfigPartition): DataFrame = {
      if (config.hasSpecialAction)
        config.name match {
          case "IR_PHA_R" => addMoleculeCombinationColumn()
          case _ => dataFrame
        }
      else
        dataFrame
    }
  }

}
