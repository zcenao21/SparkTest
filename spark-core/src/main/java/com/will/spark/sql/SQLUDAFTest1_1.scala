package com.will.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession, functions}

// DSL语法实现自定义聚合函数
object SQLUDAFTest1_1 {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        // $example on:init_session$
        val conf = new SparkConf().setMaster("local[*]").setAppName("SQLTest")
        val spark = SparkSession
            .builder()
            .config(conf)
            .appName("Spark SQL UDAF example")
            .getOrCreate()
        import spark.implicits._

        val df = spark.read.json("spark-core/src/main/resources/student.json")
        val ds:Dataset[People] = df.as[People]
        val column = new MyAvg3().toColumn
        ds.select(column).as("avg").show

        spark.stop()
    }

    case class People(name:String, age:Long)
    case class AvgClass(var total:Long, var count:Long)

    class MyAvg3 extends Aggregator[People, AvgClass, Long]{
        override def zero: AvgClass = AvgClass(0, 0)

        override def reduce(b: AvgClass, a: People): AvgClass = {
            b.total=b.total+a.age
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
