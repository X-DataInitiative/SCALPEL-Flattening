package fr.polytechnique.cmap.cnam.utilities.reporting


/**
  * Represents the reporting metadata for a single operation.
  * An operation can be any method that touches a DataFrame, including but not limited to: readers,
  *   extractors, transformers and filters.
  */
case class OperationMetadata(
    inputsTable: List[String],
    PartitionColumn: List[String],
    FormatDateInput: List[String],
    inputsPath: List[String],
    outputTable: List[String],
    outputPath: String)
  extends JsonSerializable

