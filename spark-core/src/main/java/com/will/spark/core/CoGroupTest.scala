package com.will.spark.core

import org.apache.spark.{SparkConf, SparkContext}

import scala.{+:, collection}

object CoGroupTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)
        val rddIn1 = sc.parallelize(List(("a",1),("a",2),("a",3),("b",4)),3)
        val rddIn2 = sc.parallelize(List(("c",1),("a",2),("a",3),("b",4)),2)
        rddIn1.cogroup(rddIn2).collect().foreach(println)

        rddIn1.cogroup(rddIn2).mapValues(
            i =>{
                i._1 ++: i._2
            }
        ).collect().foreach(println)

        sc.stop()
    }
}
