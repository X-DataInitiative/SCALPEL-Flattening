package fr.polytechnique.cmap.cnam.flattening

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.utilities.DFUtils._

class FlatteningConvertJoinSuite extends SharedContext {

  "run convert and join together" should "convert the dummy csv into parquet then join them in a flattened file" in {

    //Given
    val sqlCtx = sqlContext
    val path = "target/test/output/flat_table/MCO"
    val expected: DataFrame = sqlContext.read.parquet("src/test/resources/flattening/parquet-table/flat_table/MCO")

    //When
    FlatteningMainConvert.run(sqlCtx, Map("env" -> "test", "meta_bin" -> "target/meta_data.bin"))
    FlatteningMainJoin.run(sqlCtx, Map("env" -> "test", "meta_bin" -> "target/meta_data.bin"))
    val joinedDF = sqlContext.read.parquet(path).withColumn("ETA_NUM", col("ETA_NUM").cast(sql.types.StringType))

    //Then
    assert(joinedDF.sameAs(expected, true))
    FileUtils.deleteQuietly(new File("target/meta_data.bin"))
  }

}
