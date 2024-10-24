package com.stg

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class BaseTest extends AnyFunSuite with BeforeAndAfterAll {
  var spark : SparkSession = _
  var enrollments_df: DataFrame = _
  var exams_df: DataFrame = _

  override protected def beforeAll(): Unit = {
    val sparkConf = new SparkConf().setAppName("Testing").setMaster("local")
    spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val loadProp = Map("header" -> "true", "inferSchema" -> "true","quote" -> "\"", "escape" -> "\\")
    enrollments_df = Utils.getReader("csv").load("src/test/resources/enrollments.csv", loadProp)(spark)
    exams_df = Utils.getReader("csv").load("src/test/resources/exams.csv", loadProp)(spark)

  }

  override protected def afterAll(): Unit = {
    spark.close()
  }

}
