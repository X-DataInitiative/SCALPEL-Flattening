package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.flattening.SpecialFlatteningActions._


class SpecialFlatteningActionsSuite extends SharedContext {

  "addMoleculeCombinationColumn" should "add molecule combination col in the dataframe" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //given
    val pha = Seq(
      ("3400931987932", "ESCINE + BUPHENINE (CHLORHYDRATE)"),
      ("3400938130577", "RISEDRONIQUE ACIDE + CALCIUM + COLECALCIFEROL, EN SEQUENTIEL"),
      ("3400930048375", "PREPARATIONS A BASE DE BISMUTH EN ASSOCIATION")
    ).toDF("PHA_CIP_C13", "PHA_NOM_PA")

    val expected = Seq(
      ("3400931987932", "ESCINE + BUPHENINE (CHLORHYDRATE)", "BUPHENINECHLORHYDRATE_ESCINE"),
      ("3400938130577", "RISEDRONIQUE ACIDE + CALCIUM + COLECALCIFEROL, EN SEQUENTIEL", "CALCIUM_COLECALCIFEROL_ENSEQUENTIEL_RISEDRONIQUEACIDE"),
      ("3400930048375", "PREPARATIONS A BASE DE BISMUTH EN ASSOCIATION", "PREPARATIONSABASEDEBISMUTHENASSOCIATION")
    ).toDF("PHA_CIP_C13", "PHA_NOM_PA", "molecule_combination")

    //when
    val res = pha.addMoleculeCombinationColumn()

    //then
    assertDFs(expected, res)

  }
}
