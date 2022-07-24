package com.will.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLUDFTest {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        // $example on:init_session$
        val conf = new SparkConf().setMaster("local[*]").setAppName("SQLTest")
        val spark = SparkSession
            .builder()
            .config(conf)
            .appName("Spark SQL UDF example")
            .getOrCreate()

        spark.udf.register("addPrefix", (name:String)=>{
            "userName:" + name
        })
        spark.read.json("spark-core/src/main/resources/student.json").createOrReplaceTempView("student")
        spark.sql("select addPrefix(name) from student where age>10").show()

        spark.stop()
    }
}
