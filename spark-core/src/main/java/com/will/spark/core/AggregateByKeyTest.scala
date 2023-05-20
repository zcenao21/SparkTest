package com.will.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)
        val rddIn = sc.parallelize(List(("a",1),("a",2),("a",3),("b",4)),3)
        rddIn.aggregateByKey((0,0))((t,v)=>(t._1+v,t._2+1),(t1,t2)=>(t1._1+t2._1, t1._2+t2._2))
            .map(line=>(line._1, line._2._1/line._2._2))
            .collect()
            .foreach(println)
        sc.stop()
    }
}
