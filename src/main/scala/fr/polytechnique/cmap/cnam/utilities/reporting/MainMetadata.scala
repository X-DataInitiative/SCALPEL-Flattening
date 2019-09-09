package fr.polytechnique.cmap.cnam.utilities.reporting

case class MainMetadata(
  className: String,
  startTimestamp: java.util.Date,
  endTimestamp: java.util.Date,
  operations: List[OperationMetadata])
  extends JsonSerializable

