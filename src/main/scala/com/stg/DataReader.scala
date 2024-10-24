package com.stg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

trait DataReader {
  def load(path: String, prop: Map[String, String] = Map.empty)(implicit spark: SparkSession): DataFrame
}

case class LoadCSV() extends DataReader {
  override def load(path: String, prop: Map[String, String] = Map.empty)(implicit spark: SparkSession): DataFrame  = {
    spark.read.options(prop).csv(path)
  }
}






