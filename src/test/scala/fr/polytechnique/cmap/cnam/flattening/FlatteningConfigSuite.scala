package fr.polytechnique.cmap.cnam.flattening

import fr.polytechnique.cmap.cnam.SharedContext

/**
  * Created by sathiya on 21/02/17.
  */
class FlatteningConfigSuite extends SharedContext {


  "getPartitionList" should "correctly retrieves the corresponding table configuration" in {
    // Given
    val inputConfs = FlatteningConfig.tablesConfigList

    val expected = List(
      ConfigPartition("IR_BEN_R", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/IR_BEN_R.csv"),
        "target/test/output/IR_BEN_R"),
      ConfigPartition("IR_IMB_R", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/IR_IMB_R.csv"),
        "target/test/output/IR_IMB_R"),
      ConfigPartition("ER_PRS_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_PRS_F.csv"),
        "target/test/output/ER_PRS_F"),
      ConfigPartition("ER_PHA_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_PHA_F.csv"),
        "target/test/output/ER_PHA_F"),
      ConfigPartition("ER_CAM_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_CAM_F.csv"),
        "target/test/output/ER_CAM_F"),
      ConfigPartition("T_MCO__A","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO06A.csv"),
        "target/test/output/T_MCO__A/year=2006"),
      ConfigPartition("T_MCO__A","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO07A.csv"),
        "target/test/output/T_MCO__A/year=2007"),
      ConfigPartition("T_MCO__A","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08A.csv"),
        "target/test/output/T_MCO__A/year=2008"),
      ConfigPartition("T_MCO__B","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO06B.csv"),
        "target/test/output/T_MCO__B/year=2006"),
      ConfigPartition("T_MCO__B","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO07B.csv"),
        "target/test/output/T_MCO__B/year=2007"),
      ConfigPartition("T_MCO__B","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08B.csv"),
        "target/test/output/T_MCO__B/year=2008"),
      ConfigPartition("T_MCO__C","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO06C.csv"),
        "target/test/output/T_MCO__C/year=2006"),
      ConfigPartition("T_MCO__C","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO07C.csv"),
        "target/test/output/T_MCO__C/year=2007"),
      ConfigPartition("T_MCO__C","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08C.csv"),
        "target/test/output/T_MCO__C/year=2008"),
      ConfigPartition("T_MCO__D","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO06D.csv"),
        "target/test/output/T_MCO__D/year=2006"),
      ConfigPartition("T_MCO__D","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO07D.csv"),
        "target/test/output/T_MCO__D/year=2007"),
      ConfigPartition("T_MCO__D","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08D.csv"),
        "target/test/output/T_MCO__D/year=2008"),
      ConfigPartition("T_MCO__UM","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08UM.csv"),
        "target/test/output/T_MCO__UM/year=2008")
    )
    // When
    val result = FlatteningConfig.getPartitionList(inputConfs)

    // Then
    assert(result.toSet ==  expected.toSet)

  }
}
