package com.will.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLTest {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        // $example on:init_session$
        val conf = new SparkConf().setMaster("local[*]").setAppName("SQLTest")
        val spark = SparkSession
            .builder()
            .config(conf)
            .appName("Spark SQL basic example")
            .getOrCreate()
        import spark.implicits._

        spark.read.json("spark-core/src/main/resources/student.json").createOrReplaceTempView("student")
        spark.sql("select name from student where age>10").show()

        // $example off:init_session$
        //    runInferSchemaExample(spark)
        //    runProgrammaticSchemaExample(spark)

        spark.stop()
    }
}
