// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.utilities.reporting

import java.io.File
import scala.io.Source
import org.scalatest.FlatSpecLike

class MetadataReporterSuite extends FlatSpecLike {
  "writeMetaData" should "write meta data in a right place" in {
    //Given
    val mco = OperationMetadata("MCO", "/path/to/flat_table", "flat_table", List("ETA_NUM", "RSA_NUM"))
    val pha = OperationMetadata("ER_PHA_F", "/path/to/single_table", "single_table")

    val expectedMetadata = MainMetadata(
      this.getClass.getName,
      new java.util.Date(),
      new java.util.Date(),
      List(mco, pha)
    ).toJsonString()

    // When
    MetadataReporter.writeMetaData(expectedMetadata, "metadata_env1.json", "env1")
    MetadataReporter.writeMetaData(expectedMetadata, "metadata_test.json", "test")
    val metadataFileInEnv1 = new File("metadata_env1.json")
    val metadataFileInTest = new File("metadata_test.json")
    val metadata = Source.fromFile("metadata_env1.json").getLines().mkString("\n")

    //Then
    assert(metadataFileInEnv1.exists() && metadataFileInEnv1.isFile)
    assert(expectedMetadata == metadata)
    assert(!metadataFileInTest.exists())

    metadataFileInEnv1.delete()

  }
}
