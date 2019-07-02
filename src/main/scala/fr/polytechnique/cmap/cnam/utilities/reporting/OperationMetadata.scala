package fr.polytechnique.cmap.cnam.utilities.reporting


/**
  * Represents the reporting metadata for a single operation.
  * An operation can be any method that touches a DataFrame, including but not limited to: readers,
  *   extractors, transformers and filters.
  */

case class InputTable(
    inputTable: String,
    partitionColumn: Option[String],
    formatDateInput: String,
    inputPaths: List[String])

case class OperationMetadata(
    outputTable: String,
    outputPath: String,
    singleTables: List[InputTable],
    joinKeys: List[String])
  extends JsonSerializable

