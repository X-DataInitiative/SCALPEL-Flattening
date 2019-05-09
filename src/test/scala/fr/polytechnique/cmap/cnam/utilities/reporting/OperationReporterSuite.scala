package fr.polytechnique.cmap.cnam.utilities.reporting

import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.Path
import org.apache.spark.sql.SQLContext

class OperationReporterSuite extends SharedContext {

  lazy val sqlCtx: SQLContext = super.sqlContext

  "report" should "Return the correct metadata" in {

    // Given
    val path = Path("target/test/output")

    val expected = OperationMetadata(
      "mcoce",
      path.toString,
      List(new InputTable("mco_fmstc",
                  "ETA_NUM",
                  "ddMMMyyyy",
                  List("/test/input/mco_fmstc2014","/test/input/mco_fmstc2016"))),
      List("ETA_NUM","SEQ_NUM")
    )

    // When
    val result: OperationMetadata = OperationReporter.report("mcoce",
                                                              path,
                                                              List(new InputTable("mco_fmstc",
                                                                                  "ETA_NUM",
                                                                                  "ddMMMyyyy",
                                                                                  List("/test/input/mco_fmstc2014","/test/input/mco_fmstc2016"))),
                                                              List("ETA_NUM","SEQ_NUM")
      )

    // Then
    assert(result == expected)
  }
}
