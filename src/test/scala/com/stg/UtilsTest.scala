package com.stg

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

class UtilsTest extends BaseTest{

  test("get reader test") {
   val actual_reader = Utils.getReader("csv")
    assert(actual_reader.isInstanceOf[LoadCSV], "CSV Loader does not match")
  }

  test("get reader test for bad format") {
    val exception = intercept[IllegalArgumentException] {
      Utils.getReader("unsupported")
    }
    assert(exception.getMessage.contains("Unsupported format:"))
  }



  test("getOldStudents test") {

    val students_df = exams_df.select("student_name","student_code").distinct
    val actual_df = Utils.getOldStudents(3, enrollments_df, students_df)

    val data = Seq(
      Row(12053, "Mike"),
      Row(74343, "Daren"),
      Row(64155, "Keith"),
      Row(14376, "Lesley"),
      Row(11111, "")
    )

    val schema = StructType(Array(
      StructField("student_code", IntegerType, nullable = false),
      StructField("student_name", StringType, nullable = true)
    ))

    val expected_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val difference = actual_df.except(expected_df).union(expected_df.except(actual_df))
    assert(difference.isEmpty, "Contents do not match for old students!")


  }



  test("topNStudentsCode") {

    val actual_df = Utils.topNStudentsCode(10, exams_df)

    val data = Seq(
      Row(64155),
      Row(12053)
    )

    val schema = StructType(Array(
      StructField("student_code", IntegerType, nullable = false)
    ))

    val expected_df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    assert(actual_df.collect() sameElements expected_df.collect())

  }


}
