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

        spark.read.json("spark-core/src/main/resources/employee.json").createOrReplaceTempView("employee")
        spark.sql(
            """
              |   select
              |        ifnull(
              |         (select
              |        salary as 'SecondHighestSalary'
              |    from
              |    (
              |        select
              |            salary
              |            ,rank() over (order by salary desc) as r
              |        from Employee
              |    ) a where r=2 limit 1),null) as SecondHighestSalary
              |
              |""").show()
        spark.stop()
    }
}
