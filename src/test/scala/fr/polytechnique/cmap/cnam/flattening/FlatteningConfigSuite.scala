package fr.polytechnique.cmap.cnam.flattening

import com.typesafe.config.ConfigFactory
import fr.polytechnique.cmap.cnam.SharedContext


class FlatteningConfigSuite extends SharedContext {


  "getPartitionList" should "correctly retrieves the corresponding table configuration" in {
    // Given
    val inputConfs = FlatteningConfig.tablesConfigList

    val expected = List(
      PartitionConfig("IR_BEN_R", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/IR_BEN_R.csv"),
        "target/test/output/IR_BEN_R"),
      PartitionConfig("IR_IMB_R", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/IR_IMB_R.csv"),
        "target/test/output/IR_IMB_R"),
      PartitionConfig("ER_PRS_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_PRS_F.csv"),
        "target/test/output/ER_PRS_F/year=2006", PartitionStrategy.Month),
      PartitionConfig("ER_PHA_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_PHA_F.csv"),
        "target/test/output/ER_PHA_F/year=2006", PartitionStrategy.Year),
      PartitionConfig("ER_CAM_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_CAM_F.csv"),
        "target/test/output/ER_CAM_F/year=2006", PartitionStrategy.Year),
      PartitionConfig("MCO_UM","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08UM.csv"),
        "target/test/output/MCO_UM/year=2008", PartitionStrategy.Year),
      PartitionConfig("MCO_A","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO06A.csv"),
        "target/test/output/MCO_A/year=2006", PartitionStrategy.Year),
      PartitionConfig("MCO_A","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO07A.csv"),
        "target/test/output/MCO_A/year=2007", PartitionStrategy.Year),
      PartitionConfig("MCO_A","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08A.csv"),
        "target/test/output/MCO_A/year=2008", PartitionStrategy.Year),
      PartitionConfig("MCO_B","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO06B.csv"),
        "target/test/output/MCO_B/year=2006", PartitionStrategy.Year),
      PartitionConfig("MCO_B","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO07B.csv"),
        "target/test/output/MCO_B/year=2007", PartitionStrategy.Year),
      PartitionConfig("MCO_B","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08B.csv"),
        "target/test/output/MCO_B/year=2008", PartitionStrategy.Year),
      PartitionConfig("MCO_C","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO06C.csv"),
        "target/test/output/MCO_C/year=2006", PartitionStrategy.Year),
      PartitionConfig("MCO_C","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO07C.csv"),
        "target/test/output/MCO_C/year=2007", PartitionStrategy.Year),
      PartitionConfig("MCO_C","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08C.csv"),
        "target/test/output/MCO_C/year=2008", PartitionStrategy.Year),
      PartitionConfig("MCO_D","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO06D.csv"),
        "target/test/output/MCO_D/year=2006", PartitionStrategy.Year),
      PartitionConfig("MCO_D","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO07D.csv"),
        "target/test/output/MCO_D/year=2007", PartitionStrategy.Year),
      PartitionConfig("MCO_D","dd/MM/yyyy",List("src/test/resources/flattening/csv-table/PMSI/T_MCO08D.csv"),
        "target/test/output/MCO_D/year=2008", PartitionStrategy.Year)

    )
    // When
    val result = FlatteningConfig.getPartitionList(inputConfs)

    // Then
    assert(result.toSet ==  expected.toSet)

  }

  "toPartitionConfig" should "return the correct Partition config given the two config" in {
    // Given
    val tableConfig = ConfigFactory.parseString(
      """
      { name = IR_BEN_R
        partition_strategy = "none"
        partitions = [{path = [/shared/Observapur/raw_data/IR_BEN_R.CSV]}]
      }
      """.stripMargin
    )
    val partitionConfig = ConfigFactory.parseString("{path = [/shared/Observapur/raw_data/IR_BEN_R.CSV]}")

    val expected = PartitionConfig("IR_BEN_R","dd/MM/yyyy",List("/shared/Observapur/raw_data/IR_BEN_R.CSV"),
      "target/test/output/IR_BEN_R")

    // When
    val result = FlatteningConfig.toConfigPartition(tableConfig, partitionConfig)

    // Then
    assert(result == expected)
  }

  "SinglePartitionConfig.outputPath" should "return correct path given year strategy" in {
    // Given
    val partitionConfig =  ConfigFactory.parseString(
      """
        {path = [/shared/Observapur/raw_data/IR_BEN_R.CSV]
        year=2010}
      """.stripMargin)

    val strategy = PartitionStrategy.Year
    val tableName = "IR_BEN_R"

    val expected = "target/test/output/IR_BEN_R/year=2010"

    // When
    import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.SinglePartitionConfig
    val result = partitionConfig.outputPath(strategy, tableName)

    // Then
    assert(result == expected)
  }

  it should "return correct path given no strategy" in {
    // Given
    val partitionConfig =  ConfigFactory.parseString(
      """
        {path = [/shared/Observapur/raw_data/IR_BEN_R.CSV]}
      """.stripMargin)

    val strategy = PartitionStrategy.None
    val tableName = "IR_BEN_R"

    val expected = "target/test/output/IR_BEN_R"

    // When
    import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.SinglePartitionConfig
    val result = partitionConfig.outputPath(strategy, tableName)

    // Then
    assert(result == expected)
  }
}
