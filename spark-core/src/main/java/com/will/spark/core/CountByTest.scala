package com.will.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object CountByTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)

        // 结果：Map(4 -> 1, 2 -> 1, 1 -> 1, 3 -> 1)
        val rddIn = sc.parallelize(List(1,2,3,4),2)
        val intToLong = rddIn.countByValue()
        println(intToLong)

        // 结果：Map(b -> 1, a -> 3)
        val rddIn1 = sc.parallelize(List(("a",1),("a",2),("a",3),("b",4)),2)
        val stringToLong1 = rddIn1.countByKey()
        println(stringToLong1)

        sc.stop()
    }
}
