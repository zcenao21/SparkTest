package com.will.spark.core

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object LineageTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)
        val fs = FileSystem.get(sc.hadoopConfiguration)
        if(fs.exists(new Path("out"))){
            fs.delete(new Path("out"))
        }
        val rddIn = sc.textFile("spark-core/src/main/resources/input.txt",1)
        println(rddIn.toDebugString)
        println("***************************")
        val rdd1 = rddIn.flatMap(_.split(" ")).map(line=>(line,1))
        println(rdd1.toDebugString)
        println("***************************")
        val rdd2=rdd1.reduceByKey(_+_)
        println(rdd2.toDebugString)
        println("***************************")
        rdd2.collect().foreach(println)
        sc.stop()
    }
}
