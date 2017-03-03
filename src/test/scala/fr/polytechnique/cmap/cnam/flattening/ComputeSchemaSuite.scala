package fr.polytechnique.cmap.cnam.flattening

import java.io.File
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 03/03/17.
  */
class ComputeSchemaSuite extends SharedContext {

  "computeSchema" should "read the column formats of the tables on all the available year " +
    "and compute the appropriate DataTypes of the columns" in {

    //Given
    val expectedResultPath = "src/test/resources/flattening/expected/computedSchema"

    import fr.polytechnique.cmap.cnam.utilities.FileSystemUtils._
    val expectedResult: Seq[DataFrame]= readAllCsvDFsUnder(expectedResultPath, sqlContext)

    //When
    ComputeSchema.computeTableSchemas(sqlContext)
    val result = readAllCsvDFsUnder(ComputeSchemaConfig.outputPath, sqlContext)

    //Then
    println("ExpectedResult DFs count: " + expectedResult.size)
    println("Result DFs count: " + result.size)
    import fr.polytechnique.cmap.cnam.utilities.RichDataFrames._
    for(i <- 0 to Math.max(result.size - 1,  expectedResult.size - 1))
      assert(result.apply(i) ==== expectedResult.apply(i))

  }

}
