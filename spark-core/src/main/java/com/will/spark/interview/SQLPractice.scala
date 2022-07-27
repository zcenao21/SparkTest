package com.will.spark.interview

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLPractice {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        val spark = initEnv()

        // getAllStudentsAllGradesGTAvg(spark)
        // activeUserAvgAge(spark)

        spark.stop()
    }



    /**
     * 活跃用户平均年龄
     * 活跃用户指连续两天登录
     */
    def activeUserAvgAge(spark:SparkSession): Unit ={
        spark.read.option("header",true).csv("spark-core/src/main/resources/interview/active_user.csv").createOrReplaceTempView("user")
        spark.sql(
            """
              |select
              |    avg(age)
              |from
              |(
              |    select
              |        distinct user
              |        ,age
              |    from
              |    (
              |        select
              |            user
              |            ,age
              |            ,datediff(date,lag(date, 1, "1970-01-01") over (partition by user order by date asc)) diff
              |        from user
              |    ) a where diff=1
              |) b
              |""".stripMargin
        ).show()
    }

    /**
     * 得到所有科目成绩都大于该科目平均成绩的学生
     */
    def getAllStudentsAllGradesGTAvg(spark:SparkSession): Unit ={
        import spark.implicits._
        spark.read.option("header",true).csv("spark-core/src/main/resources/interview/grades.csv").createOrReplaceTempView("grade")
        spark.sql(
            """
              |select
              |    id
              |from
              |(
              |    select
              |        id
              |        ,grade
              |        ,avg(grade) over (partition by course ) as avg_grade
              |    from grade
              |) a group by id having sum(if(grade>avg_grade,0,1))=0
              |""".stripMargin
        ).show()
    }
    case class StudentGrade(id:String, course:String, grade:Long)

    def initEnv(): SparkSession ={
        val conf = new SparkConf().setMaster("local[*]").setAppName("SQLTest")
        val spark = SparkSession
            .builder()
            .config(conf)
            .appName("SQL Interview Test")
            .getOrCreate()
        import spark.implicits._
        spark
    }
}
