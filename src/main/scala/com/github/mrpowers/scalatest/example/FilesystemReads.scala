package com.github.mrpowers.scalatest.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object FilesystemReads extends SparkSessionWrapper {

  def untestableCode(): DataFrame = {
    val df = spark.read.option("header", "true").csv("/some/path/in/s3/")
    df.withColumn("hi", lit("hello"))
  }

  def testableCode1(path: String = "/some/path/in/s3/"): DataFrame = {
    val df = spark.read.option("header", "true").csv(path)
    df.withColumn("hi", lit("hello"))
  }

  def testableCode2(path: String = Config.get("dogPath")): DataFrame = {
    val df = spark.read.option("header", "true").csv(path)
    df.withColumn("hi", lit("hello"))
  }

  def testableCode3(
    df: DataFrame = spark.read.option("header", "true").csv(Config.get("dogPath"))
  ): DataFrame = {
    df.withColumn("hi", lit("hello"))
  }

  def withHi()(df: DataFrame): DataFrame = {
    df.withColumn("hi", lit("hello"))
  }

  def testableCode4(
    df: DataFrame = spark.read.option("header", "true").csv(Config.get("dogPath"))
  ): DataFrame = {
    df.transform(withHi())
  }

}
