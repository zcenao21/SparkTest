package com.will.spark.interview

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLPractice {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        val spark = initEnv()

        getAllStudentsAllGradesGTAvg(spark)

        spark.stop()
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
