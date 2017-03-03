package fr.polytechnique.cmap.cnam.flattening

import scala.collection.JavaConverters._
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by sathiya on 03/03/17.
  */
object ComputeSchemaConfig {

  private lazy val conf: Config = {
    //TODO: Find a cleaner way to do the same
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext
    val configPath: String = sqlContext.getConf("conf", "")
    val environment: String = sqlContext.getConf("env", "test")

    val defaultConfig = ConfigFactory.parseResources("flattening/config/compute_schema.conf").resolve().getConfig(environment)
    val newConfig = ConfigFactory.parseFile(new java.io.File(configPath)).resolve()

    newConfig.withFallback(defaultConfig).resolve()
  }

  val envName: String = conf.getString("env_name")
  val outputPath: String = conf.getString("output_path")

  val rawSchemaFilePath: List[String] = conf.getStringList("raw_schema_file_path").asScala.toList

  val databaseTablesMap: Map[String, Seq[String]] = conf.getConfigList("database_tables").asScala.toList
    .map(
      (dbTablesMap: Config) =>
        (
          dbTablesMap.getString("database_name"),
          dbTablesMap.getStringList("tables_id").asScala.toList
        )
    ).toMap

  override def toString: String = {
    s"envName -> $envName \n" +
      s"outputPath -> $outputPath \n" +
      s"rawSchemaFilePath -> $rawSchemaFilePath \n" +
      s"databaseTablesMap -> $databaseTablesMap"
  }
}
