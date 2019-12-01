package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.YearAndMonths
import fr.polytechnique.cmap.cnam.utilities.DFUtils._
import fr.polytechnique.cmap.cnam.utilities.Functions._
import org.apache.spark.sql.DataFrame
import org.mockito.Mockito._

class PMSIFlatTableSuite extends SharedContext {

  "tableCentrale" should "returns correct results" in {

    val path_singles = "../../../resources/flattening/parquet-table/single_table/"
    val path_PMSI_flat = "../../../resources/flattening/parquet-table/flat_table/PMSI_Flat/"

    val path_MCO_B = path_singles + "/MCO_B/year=2008/part-00000-ccdc70ba-9b46-49df-93cf-de3da9d7b18b-c000.parquet"
    val path_MCO_C = path_singles + "/MCO_C/year=2008/NUM_ENQ=Patient_02/part-00000-2e2334d1-dfe3-470d-a73e-29ab02d6be16.c000.parquet"

    val MCO_B = spark.read.parquet(path_MCO_B)
    val MCO_C = spark.read.parquet(path_MCO_C).addPrefix("MCO_C", )

    val tableCentrale = spark.read.parquet(path_PMSI_flat + "table_centrale.parquet")

    filterByYearAndAnnotate("2008","MCO_B")

    assert(result sameAs tableCentrale)

  }
}
