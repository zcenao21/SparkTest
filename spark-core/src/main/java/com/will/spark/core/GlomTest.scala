package com.will.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object GlomTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)
        val rddIn = sc.parallelize(List(1,2,3,4),3)

        // glom实现
        val re = rddIn.glom().collect()
        re.foreach(line=>{
           println(line.mkString(", "))
        })

        // mapPartitionsWithIndex实现
        val res= rddIn.mapPartitionsWithIndex((index, line)=>{
            Iterator(index + ":" + line.mkString(", "))
        }).collect()
        res.foreach(line=>{
            println(line)
        })
        sc.stop()
    }
}
