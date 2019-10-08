// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.utilities.reporting

import java.io.File
import org.apache.commons.io.FileUtils
import org.scalatest.FlatSpecLike

class OperationMetadataSuite extends FlatSpecLike {
  "serialize/deserialize" should "serialize or deserialize objects" in {
    val file = "target/meta_data.bin"
    val mco = OperationMetadata("MCO", "/path/to/flat_table", "flat_table", List("ETA_NUM", "RSA_NUM"))
    val pha = OperationMetadata("ER_PHA_F", "/path/to/single_table", "single_table")
    OperationMetadata.serialize(file, Map("MCO" -> mco, "ER_PHA_F" -> pha))
    val res = OperationMetadata.deserialize(file)
    assert(mco == res("MCO"))
    assert(pha == res("ER_PHA_F"))
    FileUtils.deleteQuietly(new File(file))
  }
}
