package com.will.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

object SQLDataLoadAndSave {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        // $example on:init_session$
        val conf = new SparkConf().setMaster("local[*]").setAppName("SQLTest")
        val spark = SparkSession
            .builder()
            .config(conf)
            .appName("Spark SQL UDAF example")
            .getOrCreate()

        // load方式的通用读取，默认parquet
        val df = spark.read.format("json").load("spark-core/src/main/resources/student.json")
//        df.show()

//        // 特定格式的保存，默认parquet
//        df.write.save("spark-core/src/main/resources/student_parquet/")
//
//        // 特定格式的读取
//        val df1 = spark.read.load("spark-core/src/main/resources/student_parquet/")
//        df1.show

//        spark.sql("select * from json.`spark-core/src/main/resources/student.json`").show
//        println("=========")
//        spark.sql("select * from parquet.`spark-core/src/main/resources/student_parquet/`").show

        df.write.mode("overwrite").save("out")
        spark.stop()
    }
}
