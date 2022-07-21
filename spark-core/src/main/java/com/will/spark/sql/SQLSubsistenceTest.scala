package com.will.spark.sql

import org.apache.spark.sql.SparkSession

object SQLSubsistenceTest {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        // $example on:init_session$
        val spark = SparkSession
            .builder()
            .appName("Spark SQL basic example")
            .getOrCreate()

        spark.read.json("spark-core/src/main/resources/access.json").createOrReplaceTempView("access")

        //select
        //    a.time
        //    , base
        //    , d1Sub
        //    , d2Sub
        //    , round(a.d1Sub*1.0/a.base,2) d1Rate
        //    , round(a.d2Sub*1.0/a.base,2) d2Rate
        //from
        //(
        //    select
        //        a.time
        //        ,sum(if(datediff(b.time, a.time)=0,1,0)) as base
        //        ,sum(if(datediff(b.time, a.time)=1,1,0)) as d1Sub
        //        ,sum(if(datediff(b.time, a.time)=2,1,0)) as d2Sub
        //    from access a
        //    join access b on a.name=b.name
        //    group by a.time
        //    order by a.time
        //) a
        spark.sql(
        "\nselect\n    a.time\n    , base\n    , d1Sub\n    , d2Sub\n    , round(a.d1Sub*1.0/a.base,2) d1Rate\n    , round(a.d2Sub*1.0/a.base,2) d2Rate\nfrom\n(\n    select\n        a.time\n        ,sum(if(datediff(b.time, a.time)=0,1,0)) as base\n        ,sum(if(datediff(b.time, a.time)=1,1,0)) as d1Sub\n        ,sum(if(datediff(b.time, a.time)=2,1,0)) as d2Sub\n    from access a\n    join access b on a.name=b.name\n    group by a.time\n    order by a.time\n) a"
         ).show()

        spark.stop()
    }
}
