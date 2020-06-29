package fr.polytechnique.cmap.cnam

import fr.polytechnique.cmap.cnam.flattening.tables.AnyTable

package object flattening {
  type Schema = Map[String, List[(String, String)]]

  type TableCategory[A <: AnyTable] = String
}
