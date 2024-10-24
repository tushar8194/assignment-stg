package com.stg

class DataReadereTest extends BaseTest {

  test("Reading csv file test") {

    val loadProp = Map("header" -> "true", "inferSchema" -> "true")
    val testFile = "src/test/resources/testdata.csv"
    val actual_df = Utils.getReader("csv").load(testFile, loadProp)(spark)

    val expected_df = spark.read.option("header","true").option("inferSchema","true").csv(testFile)

    assert(actual_df.schema == expected_df.schema, "Schemas do not match!")

    val difference = actual_df.except(expected_df).union(expected_df.except(actual_df))
    assert(difference.isEmpty, "Contents do not match!")

  }
}
