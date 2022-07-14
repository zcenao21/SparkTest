package com.will.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object TextFileTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)
        val rddNew = sc.parallelize(List(1,2,3,4))
        val rddIn = sc.textFile("spark-core/src/main/resources/input.txt")
        rddNew.saveAsTextFile("out")
//        rddIn
//            .flatMap(line => {
//                line.split(" ")
//            })
//            .map(line => (line, 1))
//            .reduceByKey(_+_)
//            .foreach(println)
        sc.stop()
    }
}
