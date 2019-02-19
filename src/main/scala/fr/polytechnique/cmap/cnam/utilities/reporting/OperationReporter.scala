package fr.polytechnique.cmap.cnam.utilities.reporting

import fr.polytechnique.cmap.cnam.util.Path
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
    * @param operationName    The unique name (ex: "diagnoses")
    * @param operationInputs  The unique names of the previous operations on which this one depends
    * @param outputType       The type of the operation output
    * @param basePath         The base path where the data and patients will be written
    * @param saveMode         The strategy of output data(default = overwrite)
    * @return an instance of OperationMetadata
    */
  def report(
    operationName: String,
    operationInputs: List[String],
    outputType: OperationType,
    basePath: Path,
    saveMode: String = "errorIfExists"): OperationMetadata = {

    logger.info(s"""=> Reporting operation "$operationName" of output type "$outputType"""")

    val dataPath: Path = Path(basePath, operationName, "data")

    val baseMetadata = OperationMetadata(operationName, operationInputs, outputType, None, None)

    outputType match {
      case OperationTypes.AnyEvents =>
        baseMetadata.copy(
          outputPath = Some(dataPath.toString)
        )
      }
  }
}
