package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.flattening.references.IRPHARActions

trait TableColumnsActions {
  val functions: Map[String, (DataFrame) => DataFrame]

  def doActions(dataFrame: DataFrame, config: ConfigPartition): DataFrame = config.actions.foldLeft(dataFrame) {
    case (dataFrame, action) => functions.getOrElse(action, (df: DataFrame) => df)(dataFrame)
  }
}


object TableColumnsActions {

  implicit class TableColumnsActionsDataFrame(dataFrame: DataFrame) {

    def processActions(config: ConfigPartition): DataFrame = {
      config.name match {
        case "IR_PHA_R" => IRPHARActions.doActions(dataFrame, config)
        case _ => dataFrame
      }
    }
  }

}
