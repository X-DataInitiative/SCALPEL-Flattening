package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.SharedContext
import org.apache.spark.sql.DataFrame
import fr.polytechnique.cmap.cnam.flattening.utilities.RichDataFrames._
import org.apache.spark.sql.functions._
/**
  * Created by admindses on 20/02/2017.
  */
class FlatteningMainSuite extends SharedContext {

  "joinTables" should "return a new dataframe result of joining cenntral parquet file with the others" in {
    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val inputCentralTable: DataFrame = Seq(
      (Some("1"), Some("5"), None),
      (Some("2"), None, Some("3")),
      (Some("3"), Some("2"), Some("1")),
      (Some("4"), None, None)
    ).toDF("PatientID", "codep", "dab")
    val inputTable1ToJoin: DataFrame = Seq(
      (Some("1"), Some("anti diabetic"), None, Some("1")),
      (Some("2"), Some("anti fever"), Some("3"), Some("4")),
      (Some("3"), None, Some("1"), Some("4")),
      (Some("4"), None, None, Some("1")),
      (Some("5"), None, None, Some("2"))
    ).toDF("medicament", "descriptionDoc", "dosage","PatientID")
    val inputTable2Join: DataFrame = Seq(
      (Some("1"), Some("fever"), None, Some("3")),
      (Some("2"), Some("cancer"), Some("CIP13"), Some("1")),
      (Some("3"), Some("diabete"), Some("CIP125"), Some("4")),
      (Some("4"), None, None, Some("3")),
      (Some("5"), None, None, Some("2"))
    ).toDF("disease", "description", "code","PatientID")
    val expected: DataFrame = Seq(
      (Some("1"), Some("5"), None, Some("1"), Some("anti diabetic"), None, Some("2"), Some("cancer"), Some("CIP13")),
      (Some("2"), None, Some("3"), Some("5"), None, None, Some("5"), None, None),
      (Some("4"), None, None, Some("2"), Some("anti fever"), Some("3"), Some("3"), Some("diabete"), Some("CIP125")),
      (Some("4"), None, None, Some("3"), None, Some("1"), Some("3"), Some("diabete"), Some("CIP125")),
      (Some("1"), Some("5"), None, Some("4"), None, None, Some("2"), Some("cancer"), Some("CIP13")),
      (Some("3"), Some("2"), Some("1"), None, None, None, Some("1"), Some("fever"), None),
      (Some("3"), Some("2"), Some("1"), None, None, None, Some("4"), None, None)
    ).toDF("PatientID", "codep", "dab", "input1_medicament", "input1_descriptionDoc","input1_dosage", "input2_disease", "input2_description", "input2_code")
    def addPrefix(jt: joinTable): joinTable = {
        val dfRenamed = jt.df.toDF(jt.df.columns.map(x =>
            if (!jt.foreighKey.contains(x))
              {jt.name+ "_" + x}
            else {x})
          : _*)
     joinTable(jt.name, dfRenamed, jt.foreighKey)
    }
    def joinTables(mainTable: joinTable, tablesToJoin: List[joinTable]) = {
      tablesToJoin.foldLeft(mainTable)( (acc:joinTable,other:joinTable) =>
        joinTable(mainTable.name, acc.df.join(other.df, mainTable.foreighKey,"left_outer"), List("")))
    }

    //When
    val listeTables = (List(joinTable("input1", inputTable1ToJoin,List("PatientID")),joinTable("input2", inputTable2Join,List("PatientID")))).map(x => addPrefix(x))
    listeTables.foreach(x => x.df.show())
    val foreignKeys = List("patient","patient")
    val mainTablesListDF = (joinTable("central", inputCentralTable,List("PatientID")))
    val result = joinTables(mainTablesListDF,listeTables)

    //Then
    result.df.orderBy("patientId").show()
    expected.orderBy("patientId").show()
    assert(result.df ===  expected)

    // uncomment when want to write results
    //expected.repartition(1).write.parquet("src/test/resources/Flattening/joinedTable")
    //inputCentralTable.repartition(1).write.parquet("src/test/resources/flattening/join/inputCentralTable")
    //inputTable1ToJoin.repartition(1).write.parquet("src/test/resources/flattening/join/inputTableToJoin/1")
    //inputTable2Join.repartition(1).write.parquet("src/test/resources/flattening/join/inputTableToJoin/2")
  }

  "run" should "write a new dataframe result of joining cenntral parquet file with the others" in {

    val sqlCtx = sqlContext
    import sqlCtx.implicits._

    //Given
    val path = "E:/flatteningProject/runResult/"
    val expected: DataFrame = Seq(
      (Some("1"), Some("5"), None, Some("1"), Some("anti diabetic"), None, Some("2"), Some("cancer"), Some("CIP13")),
      (Some("2"), None, Some("3"), Some("5"), None, None, Some("5"), None, None),
      (Some("4"), None, None, Some("2"), Some("anti fever"), Some("3"), Some("3"), Some("diabete"), Some("CIP125")),
      (Some("4"), None, None, Some("3"), None, Some("1"), Some("3"), Some("diabete"), Some("CIP125")),
      (Some("1"), Some("5"), None, Some("4"), None, None, Some("2"), Some("cancer"), Some("CIP13")),
      (Some("3"), Some("2"), Some("1"), None, None, None, Some("1"), Some("fever"), None),
      (Some("3"), Some("2"), Some("1"), None, None, None, Some("4"), None, None)
    ).toDF("PatientID", "codep", "dab", "medicament", "descriptionDoc","dosage", "disease", "description", "code")

    //When
    FlatteningMain.run(sqlCtx,Map("env" -> "test", "strategy" -> "join"))
    val joinedDF = sqlContext.read.parquet(path)

    //Then
    joinedDF.orderBy("patientId").show()
    expected.orderBy("patientId").show()
    assert(joinedDF ===  expected)


  }
}
