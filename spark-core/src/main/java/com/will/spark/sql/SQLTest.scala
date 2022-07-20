package com.will.spark.sql

import org.apache.spark.sql.SparkSession

object SQLTest {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        // $example on:init_session$
        val spark = SparkSession
            .builder()
            .appName("Spark SQL basic example")
            .config("spark.some.config.option", "some-value")
            .getOrCreate()

        spark.read.json("spark-core/src/main/resources/student.json").createOrReplaceTempView("student")
        spark.sql("select name from student where age>1").show()

        // $example off:init_session$
        //    runInferSchemaExample(spark)
        //    runProgrammaticSchemaExample(spark)

        spark.stop()
    }
    //
    //    private def runInferSchemaExample(spark: SparkSession): Unit = {
    //        // $example on:schema_inferring$
    //        // For implicit conversions from RDDs to DataFrames
    //        import spark.implicits._
    //
    //        // Create an RDD of Person objects from a text file, convert it to a Dataframe
    //        val peopleDF = spark.sparkContext
    //            .textFile("examples/src/main/resources/people.txt")
    //            .map(_.split(","))
    //            .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    //            .toDF()
    //        // Register the DataFrame as a temporary view
    //        peopleDF.createOrReplaceTempView("people")
    //
    //        // SQL statements can be run by using the sql methods provided by Spark
    //        val teenagersDF = spark.sql("SELECT name, age FROM people WHERE age BETWEEN 13 AND 19")
    //
    //        // The columns of a row in the result can be accessed by field index
    //        teenagersDF.map(teenager => "Name: " + teenager(0)).show()
    //        // +------------+
    //        // |       value|
    //        // +------------+
    //        // |Name: Justin|
    //        // +------------+
    //
    //        // or by field name
    //        teenagersDF.map(teenager => "Name: " + teenager.getAs[String]("name")).show()
    //        // +------------+
    //        // |       value|
    //        // +------------+
    //        // |Name: Justin|
    //        // +------------+
    //
    //        // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    //        implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    //        // Primitive types and case classes can be also defined as
    //        // implicit val stringIntMapEncoder: Encoder[Map[String, Any]] = ExpressionEncoder()
    //
    //        // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    //        teenagersDF.map(teenager => teenager.getValuesMap[Any](List("name", "age"))).collect()
    //        // Array(Map("name" -> "Justin", "age" -> 19))
    //        // $example off:schema_inferring$
    //    }
    //
    //    private def runProgrammaticSchemaExample(spark: SparkSession): Unit = {
    //        import spark.implicits._
    //        // $example on:programmatic_schema$
    //        // Create an RDD
    //        val peopleRDD = spark.sparkContext.textFile("examples/src/main/resources/people.txt")
    //
    //        // The schema is encoded in a string
    //        val schemaString = "name age"
    //
    //        // Generate the schema based on the string of schema
    //        val fields = schemaString.split(" ")
    //            .map(fieldName => StructField(fieldName, StringType, nullable = true))
    //        val schema = StructType(fields)
    //
    //        // Convert records of the RDD (people) to Rows
    //        val rowRDD = peopleRDD
    //            .map(_.split(","))
    //            .map(attributes => Row(attributes(0), attributes(1).trim))
    //
    //        // Apply the schema to the RDD
    //        val peopleDF = spark.createDataFrame(rowRDD, schema)
    //
    //        // Creates a temporary view using the DataFrame
    //        peopleDF.createOrReplaceTempView("people")
    //
    //        // SQL can be run over a temporary view created using DataFrames
    //        val results = spark.sql("SELECT name FROM people")
    //
    //        // The results of SQL queries are DataFrames and support all the normal RDD operations
    //        // The columns of a row in the result can be accessed by field index or by field name
    //        results.map(attributes => "Name: " + attributes(0)).show()
    //        // +-------------+
    //        // |        value|
    //        // +-------------+
    //        // |Name: Michael|
    //        // |   Name: Andy|
    //        // | Name: Justin|
    //        // +-------------+
    //        // $example off:programmatic_schema$
    //    }
}
