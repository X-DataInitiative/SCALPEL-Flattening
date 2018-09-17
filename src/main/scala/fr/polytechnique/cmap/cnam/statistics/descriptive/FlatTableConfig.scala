package fr.polytechnique.cmap.cnam.statistics.descriptive

case class FlatTableConfig(
  name: String,
  centralTable: String,
  joinKeys: List[String] = List.empty[String],
  dateFormat: String = "dd/MM/yyyy",
  inputPath: String,
  outputStatPath: String,
  singleTables: List[SingleTableConfig] = List.empty[SingleTableConfig]) {

  override def toString: String = {
    s"tableName -> $name \n" +
      s"centralTable -> $centralTable \n" +
      s"joinKeys -> $joinKeys \n" +
      s"dateFormat -> $dateFormat \n" +
      s"inputPath -> $inputPath \n" +
      s"outputStatPath -> $outputStatPath \n" +
      s"singleTableCount -> ${singleTables.size}"
  }

}
