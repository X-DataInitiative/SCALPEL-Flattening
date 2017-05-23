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

  val describeOldFlatTable: Boolean = conf.getBoolean("describe_old")

  val oldFlatConfig: List[FlatTableConfig] = {
    if(conf.hasPath("old_flat")) {
      conf.getConfigList("old_flat").asScala.toList.map(FlatTableConfig.fromConfig)
    } else {
      List[FlatTableConfig]()
    }
  }

  val mainFlatConfig: List[FlatTableConfig] = {
    if(conf.hasPath("main_flat")) {
      conf.getConfigList("main_flat").asScala.toList.map(FlatTableConfig.fromConfig)
    } else {
      List[FlatTableConfig]()
    }
  }
}
