package com.will.spark.interview

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SQLPractice {
    case class Person(name: String, age: Long)

    def main(args: Array[String]): Unit = {
        val spark = initEnv()

        revRank(spark)

        spark.stop()
    }

    /**
     * 将每个用户的所有粉丝列表逆序
     * @param spark
     */
    def revRank(spark: SparkSession): Unit = {
        spark.read.option("header", true).csv("spark-core/src/main/resources/interview/fans_rev.csv").createOrReplaceTempView("fans")

        //              |select
        //              |    uid
        //              |    ,concat_ws("",collect_list(fan)) as fan_list_rev
        //              |from
        //              |(
        //              |    select
        //              |        a.uid
        //              |        ,a.fan
        //              |    from
        //              |    (
        //              |        select
        //              |            uid
        //              |            ,r
        //              |            ,fan
        //              |        from fans
        //              |        lateral view posexplode(split(fan_list,"")) as r,fan
        //              |        where length(fan)>0
        //              |    ) a
        //              |    distribute by uid sort by r desc
        //              |) a
        //              |group by 1
        //              |order by 1 asc
    }

    /**
     * 将用户好友按照分数聚合在一行，并按分数降序排序
     * 有个用户好友表：字段如下
     * uid  fans_uid   score
     * 返回：uid, fans_uid_list【fans_uid的拼接串，按照score降序拼接】
     *
     * 比如返回如下结果：
     *  1	[c,d,a,e,b]
     *  2	[e,a,b,d,c]
     *  3	[a,c,b]
     */
    def fansByScoreRank(spark: SparkSession): Unit = {
        spark.read.option("header", true).csv("spark-core/src/main/resources/interview/fans.csv").createOrReplaceTempView("fans")
//        spark.sql(
//            """
//              |
//              |with tmp_fan as (
//              |    select
//              |        uid
//              |        ,fans_uid
//              |        ,row_number() over (partition by uid order by score desc) as r
//              |    from fans
//              |)
//              |
//              |select
//              |    uid
//              |    ,split(regexp_replace(concat_ws(":",array_sort(collect_list(concat(lpad(r,5,'0'),fans_uid)))),"0+\\d",""),":") as fan_list
//              |from tmp_fan
//              |group by 1
//              |order by 1
//              |""".stripMargin)
//            .show()

//        spark.sql(
//            """
//              |select
//              |    uid
//              |    ,collect_list(fans_uid)
//              |from
//              |(
//              |    select
//              |        uid
//              |        ,fans_uid
//              |    from fans
//              |    distribute by uid
//              |    sort by uid asc,score desc
//              |) a
//              |group by uid
//              |""".stripMargin)
//            .explain()

        spark.sql(
            """
              |
              |select
              |    uid
              |    ,split(regexp_replace(concat_ws(":",array_sort(collect_list(concat(concat(lpad(r,5,'0'),"-"),fans_uid)))),"\\d+-",""),":") fans_list
              |from
              |(
              |    select
              |        uid
              |        ,fans_uid
              |        ,row_number() over (partition by uid order by cast(score as int) desc) as r
              |    from fans
              |) a
              |group by 1
              |order by 1
              |
              |""".stripMargin
        ).show()

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
     * 没有数据返回null
     * 总共一条，没有第二高 返回null
     * 总共多条，但是薪水一样 返回null
     * 第二有多条一样的数据，返回一条
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
    def continuesLogin(spark: SparkSession): Unit = {
        spark.read.option("header", true).csv("spark-core/src/main/resources/interview/login_log.csv").createOrReplaceTempView("login")
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
    def activeUserAvgAge(spark: SparkSession): Unit = {
        spark.read.option("header", true).csv("spark-core/src/main/resources/interview/active_user.csv").createOrReplaceTempView("user")
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
    def getAllStudentsAllGradesGTAvg(spark: SparkSession): Unit = {
        import spark.implicits._
        spark.read.option("header", true).csv("spark-core/src/main/resources/interview/grades.csv").createOrReplaceTempView("grade")
        spark.sql(
            """
              |
              |""".stripMargin
        ).show()
    }

    case class StudentGrade(id: String, course: String, grade: Long)

    def initEnv(): SparkSession = {
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
