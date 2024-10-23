trait DataReader {
  def readData(spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame
}

case class LoadCSV(path: String, prop: Map[String, String] = Map.empty) extends DataReader {
  override def readData(spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    spark.read.options(prop).csv(path)
  }
}






