package fr.polytechnique.cmap.cnam.statistics.descriptive

import java.nio.file.Paths
import com.typesafe.config.ConfigFactory
import fr.polytechnique.cmap.cnam.SharedContext

class StatisticsConfigSuite extends SharedContext {

  "load" should "load the correct config file" in {
    //given
    val defaultConfig = StatisticsConfig.load("", "test")
    val flatTableConfig = FlatTableConfig(name = "name", centralTable = "central_table",
      dateFormat = "yyyy-MM-dd", inputPath = "in", outputStatPath = "out")
    val expected = defaultConfig.copy(describeOld = false, oldFlat = List(flatTableConfig))

    val stringConfig =
      """
        |describe_old = false
        |
        |old_flat = [
        |  {
        |    name = "name"
        |    central_table = "central_table"
        |    date_format = "yyyy-MM-dd"
        |    input_path = "in"
        |    output_stat_path = "out"
        |  }
        |]
      """.trim.stripMargin

    val tempPath = "target/statistics.conf"
    pureconfig.saveConfigAsPropertyFile(ConfigFactory.parseString(stringConfig), Paths.get(tempPath), true)
    //when
    val result = StatisticsConfig.load(tempPath, "test")
    //then
    assert(result == expected)

  }

}
