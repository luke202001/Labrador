package com.dyingbleed.labrador

import com.dyingbleed.labrador.report.ConsoleReport
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by 李震 on 2017/7/14.
  */
class LabradorTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("unit test")
      .master("local[*]")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }

  "count > 0 " should "success" in {
    val df = spark.createDataFrame(Seq(new User("Tom"), new User("Jerry")))

    Labrador.create(df)
      .count(count => count > 0)
      .run(new ConsoleReport)
  }

  "count > 0" should "failed" in {
    val df = spark.emptyDataFrame

    Labrador.create(df)
      .count(count => count > 0)
      .run(new ConsoleReport)
  }

  "name notnull" should "success" in {
    val df = spark.createDataFrame(Seq(new User("Tom"), new User("Jerry")))

    Labrador.create(df)
      .notNull("name")
      .run(new ConsoleReport)
  }

  "name notnull" should "failed" in {
    val df = spark.createDataFrame(Seq(new User("Tom"), new User(null)))

    Labrador.create(df)
      .notNull("name")
      .run(new ConsoleReport)
  }

  "name regexp '^\\w+\\-\\d+$'" should "success" in {
    val df = spark.createDataFrame(Seq(new User("user-12"), new User("user-21")))

    Labrador.create(df)
      .regexp("name", "^\\w+\\-\\d+$")
      .run(new ConsoleReport)
  }

  "name regexp '^\\w+\\-\\d+$'" should "failed" in {
    val df = spark.createDataFrame(Seq(new User("Tom"), new User("Jerry")))

    Labrador.create(df)
      .regexp("name", "^\\w+\\-\\d+$")
      .run(new ConsoleReport)
  }

}
