package com.will.spark.core

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AccTest {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Word Count")
        val sc = new SparkContext(conf)
        val acc = new MyAcc
        sc.register(acc)

        val rddIn = sc.parallelize(List("hello spark", "hello world"),2)
        rddIn
            .flatMap(line=>line.split(" "))
            .foreach(line=>{
                acc.add(line)
            })
        println(acc.value)

        sc.stop()
    }

    class MyAcc extends AccumulatorV2[String, mutable.HashMap[String, Long]]{
        private val map = new mutable.HashMap[String, Long]()
        override def isZero: Boolean = {
            map.isEmpty
        }

        override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
            new MyAcc
        }

        override def reset(): Unit = {
            map.clear()
        }

        override def add(v: String): Unit = {
            val newVal = map.getOrElse(v, 0L) + 1
            map.update(v, newVal)
        }

        override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
            val map = this.map;
            other.value.foreach{
                case (word ,count)=>{
                    val newVal = map.getOrElse(word, 0L) + count
                    map.update(word, newVal)
            }}
        }

        override def value: mutable.HashMap[String, Long] = {
            map
        }
    }
}
