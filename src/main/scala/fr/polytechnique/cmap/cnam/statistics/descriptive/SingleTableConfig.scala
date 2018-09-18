package fr.polytechnique.cmap.cnam.statistics.descriptive

case class SingleTableConfig(
  name: String,
  inputPath: String) {

  override def toString: String = {
    s"tableName -> $name \n" +
      s"inputPath -> $inputPath"
  }
}

