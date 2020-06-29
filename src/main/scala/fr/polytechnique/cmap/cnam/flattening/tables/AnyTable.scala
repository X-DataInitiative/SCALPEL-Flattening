package fr.polytechnique.cmap.cnam.flattening.tables

import fr.polytechnique.cmap.cnam.flattening.TableCategory

trait AnyTable {
  val category: TableCategory[AnyTable]
}
