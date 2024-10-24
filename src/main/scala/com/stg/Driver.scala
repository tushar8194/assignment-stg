package com.stg

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Driver {

  def main(args: Array[String]): Unit = {

    // ================================================Preparation======================================================

    val sparkConf = new SparkConf().setAppName("demoApp").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    implicit val implicitSpark: SparkSession = spark

    val loadProp = Map("header" -> "true", "inferSchema" -> "true")
    val enrollments_df = Utils.getReader("csv").load("src/main/resources/enrollments.csv", loadProp)
    val exams_df = Utils.getReader("csv").load("src/main/resources/exams.csv", loadProp)

    enrollments_df.printSchema()
    exams_df.printSchema()

    // ================================================Question 1=======================================================

    /* Question -1 : Provide the code and name of the students enrolled more than three years ago, if the
    name is not available please leave the name field empty. */


    val students_df = exams_df.select("student_name","student_code").distinct
    val old_students = Utils.getOldStudents(3, enrollments_df, students_df)

    old_students.write.option("header","true").mode("overwrite").csv("src/main/output/old_students/")



    // ================================================Question 2=======================================================

    /* Question -2 : Provide the student code of the 10 best students which passed more than 3 exams
    sorted descending by average of grade. An exam is considered passed with a grade
    greater or equal to 18. */


   /*  Steps :
    1. calculate students with (more than 3 exams) and (grade greater or equal to 18) from exams_df
    2. calculate avg of grade as avg_grade in exams_df
    3. sorted descending by average of grade on avg_grade
    4. top 10 student code*/

    val top_ten_students = Utils.topNStudentsCode(10, exams_df)
    top_ten_students.write.option("header","true").mode("overwrite").csv("src/main/output/top_ten_students/")


  }

}
