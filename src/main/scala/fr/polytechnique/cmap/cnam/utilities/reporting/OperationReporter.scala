package fr.polytechnique.cmap.cnam.utilities.reporting

import fr.polytechnique.cmap.cnam.utilities.Path
import org.apache.log4j.Logger

/**
  * Singleton responsible for reporting an operation execution.
  * Includes three main actions:
  *   1) writing the operation output data,
  *   2) writing the distinct patients present in the output data and
  *   3) computing counts for both datasets
  *
  * Note: An operation can be any method that touches a DataFrame, including but not limited to: readers,
  * extractors, transformers and filters.
  */
object OperationReporter {

  private val logger = Logger.getLogger(this.getClass)

  /**
    * The main method for generating the report for the given operation
    *
    * @param TableNameOutput      Name of tables output
    * @param TablePathOutput      Path where table output is
    * @param singleTables         Tables input
    * @param joinKeys             Path where tables input are
    * @param saveMode             The strategy of output data(default = overwrite)
    * @return an instance of OperationMetadata
    */
  def report(
              TableNameOutput: String,
              TablePathOutput: Path,
              singleTables: List[InputTable],
              joinKeys: List[String],
              saveMode: String = "errorIfExists"): OperationMetadata = {

    logger.info(s"""=> Reporting operation "$TableNameOutput" of output path "$TablePathOutput"""")

    val dataPath: Path = Path(TablePathOutput)

    val baseMetadata = OperationMetadata(TableNameOutput, TablePathOutput.toString, singleTables, joinKeys)

    baseMetadata.copy(
      outputPath = dataPath.toString
    )
  }
}
