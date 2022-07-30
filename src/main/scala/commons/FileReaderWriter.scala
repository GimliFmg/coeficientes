package commons

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import sparksession.SparkSessionBuilder

/**
  * Trait that contains writing and reading opts
  */
object FileReaderWriter {

  /**
    * Reader for any Data File (CSV, TXT, etc.)
    *
    * @return DataFrame of Input File
    */
  def getSparkSession: SparkSession = {
    implicit lazy val spark: SparkSession = SparkSessionBuilder.get
    spark
  }

  /**
    * Reader for any Data File (CSV, TXT, etc.)
    *
    * @param path   Path of the input File
    * @param spark  Spark Session
    * @return DataFrame of Input File
    */
  def reader(path: String, spark: SparkSession): DataFrame = {
    spark
      .read
      .format("csv")
      .options(Map("header"-> "true", "sep" -> ","))
      .load(path)
  }

  /**
    * writes file based on some given separator
    *
    * @param finalDf    dataframe to be writed
    * @param outputPath output path
    * @param format     output format
    * @param opts       writer options as a map
    * @param spark Spark Session
    * @param partCols   columns to partition
    */
  def writeDf(finalDf: DataFrame, outputPath: String, format: String,
              opts: Map[String, String], spark: SparkSession, partCols: String*): Unit =
    finalDf.repartition(4)
      .write
      .partitionBy(partCols: _*)
      .options(opts)
      .mode(SaveMode.Overwrite).format(format)
      .save(outputPath)
}
