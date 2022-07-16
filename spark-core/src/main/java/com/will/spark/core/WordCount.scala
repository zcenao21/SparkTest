package com.will.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)

        val rddIn = sc.textFile("spark-core/src/main/resources/num.txt",3)
        rddIn.saveAsTextFile("output")
        sc.stop()
    }
}
