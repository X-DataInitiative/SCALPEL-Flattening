// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.utilities.reporting

import java.io.PrintWriter

object MetadataReporter {
  /**
    * the method to output the meta data
    *
    * @param metaData meta data
    * @param path     storage path of meta data
    * @param env      running environment
    */
  def writeMetaData(metaData: String, path: String, env: String): Unit = {
    if (env != "test") {
      new PrintWriter(path) {
        write(metaData)
        close()
      }
    }
  }
}
