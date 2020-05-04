// License: BSD 3 clause

package fr.polytechnique.cmap.cnam.flattening

import java.nio.file.Paths

import com.typesafe.config.ConfigFactory
import pureconfig._
import fr.polytechnique.cmap.cnam.SharedContext
import fr.polytechnique.cmap.cnam.config.ConfigLoader
import fr.polytechnique.cmap.cnam.flattening.FlatteningConfig.{JoinTableConfig, PartitionConfig, TableConfig, YearAndMonths}

/**
 * Created by sathiya on 21/02/17.
 */
class FlatteningConfigSuite extends SharedContext with ConfigLoader {

  "load" should "loads the correct config file" in {
    val defaultConf = FlatteningConfig.load("", "test")
    val expected = defaultConf.copy(basePath = "/path/base/output", schemaFilePath = List("/path/schema"), withTimestamp = true,
      autoBroadcastJoinThreshold = Some("10m"), join = List(JoinTableConfig(name = "DCIR", inputPath = Some("/path/to/input"),
        joinKeys = List("DCT_ORD_NUM", "FLX_DIS_DTD", "FLX_EMT_NUM", "FLX_EMT_ORD", "FLX_EMT_TYP", "FLX_TRT_DTD", "ORG_CLE_NUM",
          "PRS_ORD_NUM", "REM_TYP_AFF"), mainTableName = "ER_PRS_F", tablesToJoin = List("ER_PHA_F", "ER_CAM_F"),
        monthlyPartitionColumn = Some("FLX_DIS_DTD"), flatOutputPath = Some("/path/to/flat_table"),
        saveFlatTable = false, onlyOutput = List(YearAndMonths(2006, List(3))))))
    val stringConfig =
      """
        |base_path = "/path/base/output"
        |file_format= orc
        |with_timestamp = true
        |
        |auto_broadcast_join_threshold = "10m"
        |
        |schema_file_path = [
        |  "/path/schema"
        |]
        |
        |join = [
        |  {
        |  file_format= orc
        |   name = "DCIR"
        |   input_path = "/path/to/input"
        |   monthly_partition_column = "FLX_DIS_DTD"
        |   join_keys = [
        |     "DCT_ORD_NUM"
        |     "FLX_DIS_DTD"
        |     "FLX_EMT_NUM"
        |     "FLX_EMT_ORD"
        |     "FLX_EMT_TYP"
        |     "FLX_TRT_DTD"
        |     "ORG_CLE_NUM"
        |     "PRS_ORD_NUM"
        |     "REM_TYP_AFF"
        |   ]
        |   main_table_name = "ER_PRS_F"
        |   tables_to_join = ["ER_PHA_F","ER_CAM_F"]
        |   flat_output_path = "/path/to/flat_table"
        |   save_flat_table = false
        |   only_output = [{year = 2006, months : [3]}]
        |  }
        |]
      """.trim.stripMargin

    val tempPath = "target/flattening.conf"
    pureconfig.saveConfigAsPropertyFile(ConfigFactory.parseString(stringConfig), Paths.get(tempPath), true)
    //when
    val result = FlatteningConfig.load(tempPath, "test")
    //then
    assert(result == expected)
  }


  "partitions" should "correctly retrieves the corresponding table configuration" in {
    // Given
    val conf = FlatteningConfig.load("", "test")

    val expected = List(
      ConfigPartition("IR_BEN_R", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/IR_BEN_R.csv"),
        "target/test/output/single_table/IR_BEN_R", None),
      ConfigPartition("IR_IMB_R", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/IR_IMB_R.csv"),
        "target/test/output/single_table/IR_IMB_R", None),
      ConfigPartition("ER_PRS_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_PRS_F.csv"),
        "target/test/output/single_table/ER_PRS_F/year=2006", Some("FLX_DIS_DTD")),
      ConfigPartition("ER_PHA_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_PHA_F.csv"),
        "target/test/output/single_table/ER_PHA_F/year=2006", None),
      ConfigPartition("ER_CAM_F", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/ER_CAM_F.csv"),
        "target/test/output/single_table/ER_CAM_F/year=2006", None),
      ConfigPartition("MCO_UM", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO08UM.csv"),
        "target/test/output/single_table/MCO_UM/year=2008", None),
      ConfigPartition("MCO_A", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO06A.csv"),
        "target/test/output/single_table/MCO_A/year=2006", None),
      ConfigPartition("MCO_A", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO07A.csv"),
        "target/test/output/single_table/MCO_A/year=2007", None),
      ConfigPartition("MCO_A", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO08A.csv"),
        "target/test/output/single_table/MCO_A/year=2008", None),
      ConfigPartition("MCO_B", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO06B.csv"),
        "target/test/output/single_table/MCO_B/year=2006", None),
      ConfigPartition("MCO_B", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO07B.csv"),
        "target/test/output/single_table/MCO_B/year=2007", None),
      ConfigPartition("MCO_B", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO08B.csv"),
        "target/test/output/single_table/MCO_B/year=2008", None),
      ConfigPartition("MCO_C", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO06C.csv"),
        "target/test/output/single_table/MCO_C/year=2006", Some("NUM_ENQ")),
      ConfigPartition("MCO_C", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO07C.csv"),
        "target/test/output/single_table/MCO_C/year=2007", Some("NUM_ENQ")),
      ConfigPartition("MCO_C", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO08C.csv"),
        "target/test/output/single_table/MCO_C/year=2008", Some("NUM_ENQ")),
      ConfigPartition("MCO_D", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO06D.csv"),
        "target/test/output/single_table/MCO_D/year=2006", None),
      ConfigPartition("MCO_D", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO07D.csv"),
        "target/test/output/single_table/MCO_D/year=2007", None),
      ConfigPartition("MCO_D", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/PMSI/T_MCO08D.csv"),
        "target/test/output/single_table/MCO_D/year=2008", None),
      ConfigPartition("IR_PHA_R", "dd/MM/yyyy", List("src/test/resources/flattening/csv-table/DCIR/IR_PHA_R.csv"),
        "target/test/output/single_table/IR_PHA_R", None, actions = List("addMoleculeCombinationColumn"))
    )
    // When
    val result = conf.partitions

    // Then
    assert(result.toSet == expected.toSet)

  }

  "partitions" should "return the correct Partition config given the two config" in {
    // Given
    val tableConfig = loadConfig[TableConfig](ConfigFactory.parseString(
      """
      {
        name = IR_BEN_R
        partition_strategy = "none"
        partitions = [{path = [/shared/Observapur/raw_data/IR_BEN_R.CSV]}]
      }
      """.stripMargin)
    ).right.get
    val partitionConfig =
      loadConfig[PartitionConfig](ConfigFactory.parseString("{path = [/shared/Observapur/raw_data/IR_BEN_R.CSV]}"))
        .right.get

    val expected = ConfigPartition("IR_BEN_R", "dd/MM/yyyy", List("/shared/Observapur/raw_data/IR_BEN_R.CSV"),
      "target/test/output/IR_BEN_R", None)

    // When
    val result = FlatteningConfig.toConfigPartition("target/test/output", tableConfig, partitionConfig)

    // Then
    assert(result == expected)
  }

  "SinglePartitionConfig.outputPath" should "return correct path given year strategy" in {
    // Given
    val partitionConfig = loadConfig[PartitionConfig](ConfigFactory.parseString(
      """
        {path = [/shared/Observapur/raw_data/IR_BEN_R.CSV]
        year=2010}
      """.stripMargin)
    ).right.get

    val strategyName = "year"
    val tableName = "IR_BEN_R"

    val expected = "target/test/output/IR_BEN_R/year=2010"

    // When
    val result = partitionConfig.outputPath("target/test/output", strategyName, tableName)

    // Then
    assert(result == expected)
  }

  it should "return correct path given no strategy" in {
    // Given
    val partitionConfig = loadConfig[PartitionConfig](ConfigFactory.parseString(
      """
        {path = [/shared/Observapur/raw_data/IR_BEN_R.CSV]}
      """.stripMargin)
    ).right.get

    val strategyName = "whatever"
    val tableName = "IR_BEN_R"

    val expected = "target/test/output/IR_BEN_R"

    // When
    val result = partitionConfig.outputPath("target/test/output", strategyName, tableName)

    // Then
    assert(result == expected)
  }
}
