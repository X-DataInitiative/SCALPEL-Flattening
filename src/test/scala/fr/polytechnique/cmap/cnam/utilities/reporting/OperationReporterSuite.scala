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
      List("mcoce","mco"),
      List("ETA_NUM"),
      List("ddMMMyyyy"),
      List("/test/input/mco","/test/input/mcoce"),
      List("dcir"),
      path.toString
    )

    // When
    val result: OperationMetadata = OperationReporter.report(List("mcoce","mco"),  List("ETA_NUM"),
      List("ddMMMyyyy"), List("/test/input/mco","/test/input/mcoce"), List("dcir"), path)

    // Then
    assert(result == expected)
  }
}
