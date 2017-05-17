package fr.polytechnique.cmap.cnam.statistics

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

object StatisticsConfig {

  private lazy val conf: Config = {
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")

    val defaultConfig = ConfigFactory.parseResources("statistics/main.conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()

    newConfig.withFallback(defaultConfig).resolve()
  }

  val oldFlatConfig: List[Config] = {
    if(conf.hasPath("old_flat")) {
      conf.getConfigList("old_flat").asScala.toList
    } else {
      List[Config]()
    }
  }

  val newFlatConfig: List[Config] = {
    if(conf.hasPath("new_flat")) {
      conf.getConfigList("new_flat").asScala.toList
    } else  {
      List[Config]()
    }
  }

  implicit class StatConfig(statConf: Config) {

    val flatTableName: String = statConf.getString("name")
    val mainTableName: String = statConf.getString("main_table")
    val dateFormat: String = {
      if(statConf.hasPath("date_format")) {
        statConf.getString("date_format")
      } else {
        "dd/MM/yyyy"
      }
    }

    val inputPath: String = statConf.getString("input_path")
    val statOutputPath: String = statConf.getString("output_stat_path")

    def prettyPrint: String = {
      s"flatTableName -> $flatTableName \n" +
        s"mainTableName -> $mainTableName \n" +
        s"dateFormat -> $dateFormat \n" +
        s"inputPath -> $inputPath \n" +
        s"statOutputPath -> $statOutputPath"
    }
  }
}
