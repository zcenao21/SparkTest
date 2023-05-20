package com.will.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}

object SQLUDAFTest2 {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        // $example on:init_session$
        val conf = new SparkConf().setMaster("local[*]").setAppName("SQLTest")
        val spark = SparkSession
            .builder()
            .config(conf)
            .appName("Spark SQL UDAF example")
            .getOrCreate()

        spark.udf.register("myAvg", functions.udaf(new MyAvg2()))
        spark.read.json("spark-core/src/main/resources/student.json").createOrReplaceTempView("student")
        spark.sql("select myAvg(age) from student").show()

        spark.stop()
    }

    case class AvgClass(var total:Long, var count:Long)

    class MyAvg2 extends Aggregator[Long, AvgClass, Long]{
        override def zero: AvgClass = AvgClass(0, 0)

        override def reduce(b: AvgClass, a: Long): AvgClass = {
            b.total=b.total+a
            b.count=b.count+1
            b
        }

        override def merge(b1: AvgClass, b2: AvgClass): AvgClass = {
            b1.total=b1.total+b2.total
            b1.count=b1.count+b2.count
            b1
        }

        override def finish(reduction: AvgClass): Long = {
            reduction.total/reduction.count
        }

        override def bufferEncoder: Encoder[AvgClass] = Encoders.product

        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }
}
