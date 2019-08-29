package fr.polytechnique.cmap.cnam.utilities

import org.scalatest.FlatSpecLike

class ConfigUtilsSuite extends FlatSpecLike {

  "byteStringAsBytes" should "convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes" in {
    val vKibibytes = ConfigUtils.byteStringAsBytes("1k")
    assert(vKibibytes == 1024L)
    val vMebibytes = ConfigUtils.byteStringAsBytes("5m")
    assert(vMebibytes == 5 * 1024L * 1024L)
    val vGibibytes = ConfigUtils.byteStringAsBytes("2g")
    assert(vGibibytes == 2 * 1024L * 1024L * 1024L)
    val errorValue = ConfigUtils.byteStringAsBytes(" 2 g ")
    assert(errorValue == -1)
  }

}
