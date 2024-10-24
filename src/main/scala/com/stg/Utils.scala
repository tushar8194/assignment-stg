package com.stg

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{avg, coalesce, col, count, lit}

object Utils {
  def getReader(format: String): DataReader = {
    format.toLowerCase match {
      case "csv" => LoadCSV()
      case _ => throw new IllegalArgumentException(s"Unsupported format: $format")
    }
  }

  def getOldStudents(years_back : Int, enrollments_df: DataFrame, students_df: DataFrame): DataFrame = {
    val currentYear = java.time.Year.now.getValue
    enrollments_df
      .join(students_df, Seq("student_code"), "left")
      .filter(col("enrollment_year") < (currentYear - years_back))
      .select(
        col("student_code"),
        coalesce(col("student_name"), lit("")).alias("student_name")
      )
  }

  def topNStudentsCode(top_n : Int, exams_df : DataFrame, pass_cutOff : Int, min_exams: Int): DataFrame = {
    exams_df
      .filter(col("exam_grade") >= pass_cutOff)
      .groupBy("student_code")
      .agg(
        count("exam_grade").alias("exam_count"),
        avg("exam_grade").alias("avg_grades"))
      .filter(col("exam_count") > min_exams)
      .orderBy(col("avg_grades").desc)
      .limit(top_n)
      .select("student_code")
  }

}
