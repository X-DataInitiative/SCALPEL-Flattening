// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening.tables

import org.apache.spark.sql.{DataFrame, SQLContext}
import fr.polytechnique.cmap.cnam.flattening.TableCategory
import fr.polytechnique.cmap.cnam.utilities.DFUtils

case class Table[+A <: AnyTable](name: String, df: DataFrame, category: TableCategory[A]) extends Tableable

trait TableBuilder {
  self: AnyTable =>

  def apply[A <: AnyTable](name: String, df: DataFrame): Table[A] = Table[A](name, df, self.category)

  def apply[A <: AnyTable](sqlContext: SQLContext, inputBasePath: String, name: String): Table[A] = apply(name, DFUtils.readParquet(sqlContext, inputBasePath + "/" + name))

}

trait SingleTable extends AnyTable with TableBuilder {
  override val category: TableCategory[SingleTable] = "single_table"
}

object SingleTable extends SingleTable

trait FlatTable extends AnyTable with TableBuilder {
  override val category: TableCategory[FlatTable] = "flat_table"
}

object FlatTable extends FlatTable

trait ReferenceTable extends AnyTable with TableBuilder {
  override val category: TableCategory[ReferenceTable] = "reference"
}

object ReferenceTable extends ReferenceTable

