```
Assignment answers below two questions -


1) Provide the code and name of the students enrolled more than three years ago, if the
name is not available please leave the name field empty.

3) Provide the student code of the 10 best students which passed more than 3 exams
sorted descending by average of grade. An exam is considered passed with a grade
greater or equal to 18.


Entry point is src/main/scala/com/stg/Driver.scala, which has three parts, 

first part - is to setup spark session and load given input data in data frames
second part - is calling function getOldStudents() which returns the calculation for question 1
third part - is calling function topNStudentsCode() which returns the calculation for question 2


Trait DataReader, has entry point for various types of data formats and provides a valid data reader for given data format. 
Scope of this assigement is limited to CSV data reader. Other reader can be implement the load() method accordingly.


Utils class is a place for resuable methods. 


Output folder contains the output files for two questions.


Assumption : that each file is well-formed and valid; they don't contain duplicate or inconsistent data.

```
