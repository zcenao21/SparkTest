package com.will.spark.core

import org.apache.spark.{RangePartitioner, SparkConf, SparkContext}

object GroupByTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)
        val rddIn = sc.parallelize(List(1,2,3,4),3)
        val groupRDD = rddIn.groupBy(num=>num).collect().foreach(println)
        sc.stop()
    }
}
