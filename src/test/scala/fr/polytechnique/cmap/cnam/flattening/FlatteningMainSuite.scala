// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import org.apache.spark.sql
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils._

class FlatteningMainSuite extends SharedContext {

  "run" should "convert the dummy csv into parquet then join them in a flattened file" in {

    //Given
    val sqlCtx = sqlContext
    val path = "target/test/output/flat_table/MCO"
    val expected: DataFrame = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/MCO")

    //When
    FlatteningMain.run(sqlCtx, Map("env" -> "test"))
    val joinedDF = sqlContext.read.parquet(path).withColumn("ETA_NUM", col("ETA_NUM").cast(sql.types.StringType))

    //Then
    assert(joinedDF.sameAs(expected, true))
  }
}
