package fr.polytechnique.cmap.cnam.statistics

import fr.polytechnique.cmap.cnam.SharedContext

class StatisticsConfigSuite extends SharedContext {

  "toString" should "return all the values from the test config file correctly" in {

    // Given
    val expectedResult = "flatTableName -> MCO \n" +
      "mainTableName -> MCO_C \n" +
      "dateFormat -> dd/MM/yyyy \n" +
      "inputPath -> src/test/resources/statistics/flat_table/input/newMCO \n" +
      "statOutputPath -> target/test/output/statistics/newMCO"

    // When
    import fr.polytechnique.cmap.cnam.statistics.StatisticsConfig._
    val result = StatisticsConfig.mainFlatConfig.head.prettyPrint

    println(result)
    println(expectedResult)
    assert(result === expectedResult)
  }

}
