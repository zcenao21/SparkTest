package com.will.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object AggregateTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)

        // 结果为40而不是30
        val rddIn = sc.parallelize(List(1,2,3,4),2)
        val res = rddIn.aggregate(10)(_+_,_+_)
        println(res)

        // 结果为30
        val rddIn1 = sc.parallelize(List(1,2,3,4),2)
        rddIn1.map(line=>(1,line)).aggregateByKey(10)(_+_,_+_).foreach(println)
        sc.stop()
    }
}
