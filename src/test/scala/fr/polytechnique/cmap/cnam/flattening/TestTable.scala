package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.flattening.tables.{AnyTable, TableBuilder}

trait TestTable extends AnyTable with TableBuilder {
  override val category: TableCategory[TestTable] = "test_table"
}

object TestTable extends TestTable
