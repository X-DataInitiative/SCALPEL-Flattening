package fr.polytechnique.cmap.cnam.statistics

import fr.polytechnique.cmap.cnam.{SharedContext, utilities}

class StatisticsMainSuite extends SharedContext{

  "run" should "run the overall pipeline correctly without any error" in {

    // Given
    val expectedResultPath = "src/test/resources/statistics/flat_table/expected/newMCOStat"
    val expectedResult = sqlContext.read.parquet(expectedResultPath)
    val resultPath = "target/test/output/statistics/newMCO"

    // When
    StatisticsMain.run(sqlContext, Map())
    val result = sqlContext.read.parquet(resultPath)

    // Then
    import utilities.DFUtils.CSVDataFrame
    assert(result sameAs expectedResult)

  }
}
