package com.will.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object SQLUDAFTest {
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

        spark.udf.register("myAvg", new MyAvg())
        spark.read.json("spark-core/src/main/resources/student.json").createOrReplaceTempView("student")
        spark.sql("select myAvg(age) from student").show()

        spark.stop()
    }

    case class Student(name:String, age:Int)

    class MyAvg() extends UserDefinedAggregateFunction{
        override def inputSchema: StructType = {
            StructType(
                Array(StructField("age", LongType))
            )
        }

        override def bufferSchema: StructType = {
            StructType(
                Array(
                    StructField("sum", LongType)
                    ,StructField("cnt", LongType)
                )
            )
        }

        override def dataType: DataType = LongType

        override def deterministic: Boolean = true

        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer.update(0,0L)
            buffer.update(1,0L)
        }

        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer.update(0, buffer.getLong(0)+input.getLong(0))
            buffer.update(1,buffer.getLong(1)+1)
        }

        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1.update(0, buffer1.getLong(0)+buffer2.getLong(0))
            buffer1.update(1,buffer1.getLong(1)+buffer2.getLong(1))
        }

        override def evaluate(buffer: Row): Any = {
            buffer.getLong(0)/buffer.getLong(1)
        }
    }
}
