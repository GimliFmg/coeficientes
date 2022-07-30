package sparksession

import org.apache.logging.log4j.scala.Logging

import org.apache.spark.sql.SparkSession

object SparkSessionBuilder extends Logging {
  /**
    * Get spark session for a given configuration in cluster
    *
    * @return spark session
    */
  def get: SparkSession = {
    logger.info(s"Building Spark Session for AppName: AppName")
    SparkSession.builder()
      .appName("coeficientes")
      .master("local[*]")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .enableHiveSupport()
      .getOrCreate()
  }
}
