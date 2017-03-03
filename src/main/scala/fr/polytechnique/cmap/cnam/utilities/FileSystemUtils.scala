package fr.polytechnique.cmap.cnam.utilities

import java.io.File
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by sathiya on 03/03/17.
  */
object FileSystemUtils {

  def readAllParquetDFsFrom(pathList: List[String], sqlContext: SQLContext): Seq[DataFrame] = {
    pathList.map(
      (path: String) =>
        sqlContext.read.parquet(path)
    )
  }

  def readAllCsvDFsUnder(pathRoot: String, sqlContext: SQLContext): Seq[DataFrame] = {
    import fr.polytechnique.cmap.cnam.flattening.ReadCSVTable._
    getSubDirs(pathRoot).map(
      (subDirName: String) =>
        readCSV(sqlContext, Seq(pathRoot + "/" + subDirName))
    )
  }

  def getSubDirs(pathRoot: String): Array[String] = {
    (new File(pathRoot))
      .listFiles
      .filter(_.isDirectory)
      .map(_.getName)
  }
}
