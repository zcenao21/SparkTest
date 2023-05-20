package com.will.spark.core

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object TextFileTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)
        val fs = FileSystem.get(sc.hadoopConfiguration)
        if(fs.exists(new Path("out"))){
            fs.delete(new Path("out"))
        }
        val rddIn = sc.textFile("spark-core/src/main/resources/num.txt",3)
        rddIn.saveAsTextFile("out")
        sc.stop()
    }
}
