package fr.polytechnique.cmap.cnam.flattening

import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by sathiya on 15/02/17.
  */
object FlatteningConfig {

  private lazy val conf: Config = {
    //TODO: Find a cleaner way to do the same
    val sqlContext = SQLContext.getOrCreate(SparkContext.getOrCreate())
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")

    val defaultConfig = ConfigFactory.parseResources("flattening/config/pmsi.conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()

    newConfig.withFallback(defaultConfig).resolve()
  }

  val schemaFile: List[String] = conf.getStringList("schema_file_path").asScala.toList
  val outputPath: String = conf.getString("output_path")

  val tablesConfigList : List[Config] = conf.getConfigList("tables_config").asScala.toList

  implicit class FlatteningConfigUtilities(config: Config) {

    def name: String = config.getString("name")

    def schemaName: String = config.getString("schema_name")

    def dateFormat: String = config.getString("date_format")

    def inputPaths: List[String] = config.getStringList("input_path").asScala.toList

    def partitionKey: List[String] = config.getStringList("output.key").asScala.toList

  }

  def getTableConfig(name: String): Config = {
    tablesConfigList.filter(_.name == name).head
  }
}
