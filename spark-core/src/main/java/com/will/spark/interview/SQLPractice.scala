package com.will.spark.interview

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLPractice {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        val spark = initEnv()

        // getAllStudentsAllGradesGTAvg(spark)
        // activeUserAvgAge(spark)
        //continuesLogin(spark)
        //secondHighestSalary(spark)
        //scoreRank(spark)
        consecutiveNumbers(spark)

        spark.stop()
    }

    /**
     * 连续出现的数字
     */
    def consecutiveNumbers(spark: SparkSession): Unit = {
        spark.read.option("header", true).csv("spark-core/src/main/resources/interview/logs.csv").createOrReplaceTempView("logs")
        spark.sql(
            """
              | select
              |    distinct ConsecutiveNums as ConsecutiveNums
              |from
              |(
              |    select
              |        diff
              |        ,num
              |        ,count(1) as cnt
              |        ,max(num) as ConsecutiveNums
              |    from
              |    (
              |        select
              |            num
              |            ,id-rn as diff
              |        from
              |        (
              |            select
              |                id
              |                ,num
              |                ,row_number() over (partition by num order by id asc) as rn
              |            from logs
              |        ) a
              |    ) b group by 1,2 having cnt>=3
              |) c
              |
              |""".stripMargin
//            """
//              |select
//              |    distinct if(num==lag_1_num and num==lag_2_num,num,null) ConsecutiveNums
//              |from
//              |(
//              |    select
//              |        id
//              |        ,num
//              |        ,lag(num,1) over (order by id asc) as lag_1_num
//              |        ,lag(num,2) over (order by id asc) as lag_2_num
//              |    from logs
//              |) a
//              |where if(num==lag_1_num and num==lag_2_num,num,null)!=null
//              |
//              |""".stripMargin
        ).show()
    }

    /**
     * 分数排名
     */
    def scoreRank(spark: SparkSession): Unit = {
        spark.read.option("header", true).csv("spark-core/src/main/resources/interview/scores.csv").createOrReplaceTempView("scores")
        spark.sql(
            """
              |select
              |    score
              |    ,dense_rank() over (order by score desc) as `rank`
              |from scores
              |""".stripMargin
        ).show()
    }

    /**
     * 第二高的薪水
     * 需要考虑的情况：
     *  没有数据返回null
     *  总共一条，没有第二高 返回null
     *  总共多条，但是薪水一样 返回null
     *  第二有多条一样的数据，返回一条
     */
    def secondHighestSalary(spark: SparkSession): Unit = {
        spark.read.option("header", true).csv("spark-core/src/main/resources/interview/employee.csv").createOrReplaceTempView("employee")
        spark.sql(
            """
              |select
              |        ifnull(
              |            (
              |                select
              |                    salary as `SecondHighestSalary`
              |                from
              |                (
              |                    select
              |                        salary
              |                        ,dense_rank() over (order by salary desc) as r
              |                    from Employee
              |                ) a where r=2 limit 1
              |            ),null) as SecondHighestSalary
              |""".stripMargin
        ).show()
    }


    /**
     * 连续登录天数
     * 如果中间差一天也算连续
     * 如2022-01-01， 2022-01-03， 2022-01-04为连续登录4天
     */
    def continuesLogin(spark:SparkSession): Unit ={
        spark.read.option("header",true).csv("spark-core/src/main/resources/interview/login_log.csv").createOrReplaceTempView("login")
        spark.sql(
            """
              |select
              |    c.id
              |    ,max(datediff(last_date,first_date))+1 as succ_login_date
              |from
              |(
              |    select
              |        b.id
              |        ,first_value(b.dt) over (partition by id,sum_flag) as first_date
              |        ,last_value(b.dt) over (partition by id,sum_flag) as last_date
              |    from
              |    (
              |        select
              |            a.id
              |            ,a.dt
              |            ,sum(flag) over (partition by id order by dt) as sum_flag
              |        from
              |        (
              |            select
              |                id
              |                ,dt
              |                ,if(datediff(dt,lag(dt,1,"1970-01-01") over (partition by id order by dt))<3,0,1) as flag
              |            from login
              |        ) a
              |    ) b
              |) c group by 1
              |""".stripMargin
        ).show()
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
