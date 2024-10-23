import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, coalesce, col, count, lit}

object Driver {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("demoApp").setMaster("local")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val enrollments_df = DataReaderFactory.createReader("csv","src/main/resources/enrollments.csv", Map("header" -> "true")).readData(spark)
    val exams_df = DataReaderFactory.createReader("csv","src/main/resources/exams.csv", Map("header" -> "true")).readData(spark)




    // question -1 : Provide the code and name of the students enrolled more than three years ago, if the
    //name is not available please leave the name field empty.

    val currentYear = java.time.Year.now.getValue

    val students_df = exams_df.select("student_name","student_code").distinct

    val old_students = enrollments_df
      .join(students_df, Seq("student_code"), "left")
      .filter(col("enrollment_year") < (currentYear - 3))
      .select(
        col("student_code"),
        coalesce(col("student_name"), lit("")).alias("student_name")
      )

    old_students.show(100000)

    val prop1 = Map("header" -> "true")
    old_students.write.option("header","true").mode("overwrite").csv("src/main/output/old_students/")




    //Question -2 : Provide the student code of the 10 best students which passed more than 3 exams
    //sorted descending by average of grade. An exam is considered passed with a grade
    //greater or equal to 18.


   /* 1. calculate students with (more than 3 exams) and (grade greater or equal to 18) from exams_df
    2. calculate avg of grade as avg_grade in exams_df
    3. sorted descending by average of grade on avg_grade
    4. top 10 student code*/


    val top_ten_students = exams_df
      .filter(col("exam_grade") >= 18)
      .groupBy("student_code").agg(count("exam_grade").alias("exam_count"), avg("exam_grade").alias("avg_grades"))
      .filter(col("exam_count") > 3)
      .orderBy(col("avg_grades").desc).limit(10)

    top_ten_students.select("student_code").show(20)

    val prop2 = Map("header" -> "true")
    top_ten_students.write.option("header","true").mode("overwrite").csv("src/main/output/top_ten_students/")


  }

}
