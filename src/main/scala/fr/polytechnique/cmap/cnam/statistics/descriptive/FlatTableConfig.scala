package fr.polytechnique.cmap.cnam.statistics.descriptive

case class FlatTableConfig(
  name: String,
  centralTable: String,
  joinKeys: List[String] = List.empty[String],
  dateFormat: String = "dd/MM/yyyy",
  inputPath: String,
  outputStatPath: String,
  singleTables: List[SingleTableConfig] = List.empty[SingleTableConfig],
  saveMode: String = "errorIfExists") {

  import fr.polytechnique.cmap.cnam.utilities.DFUtils.StringPath

  lazy val output: String = if (saveMode == "withTimestamp") outputStatPath.withTimestampSuffix() else outputStatPath

  override def toString: String = {
    s"tableName -> $name \n" +
      s"centralTable -> $centralTable \n" +
      s"joinKeys -> $joinKeys \n" +
      s"dateFormat -> $dateFormat \n" +
      s"inputPath -> $inputPath \n" +
      s"outputStatPath -> $outputStatPath \n" +
      s"saveMode -> $saveMode \n" +
      s"singleTableCount -> ${singleTables.size}"
  }

}
