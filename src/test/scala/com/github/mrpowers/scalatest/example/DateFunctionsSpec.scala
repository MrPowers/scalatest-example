package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import java.sql.Date

import org.apache.spark.sql.types.{DateType, IntegerType}
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import DateFunctions._
import com.github.mrpowers.spark.fast.tests.ColumnComparer

class DateFunctionsSpec extends FunSpec with SparkSessionTestWrapper with ColumnComparer {

  it("shows how to create a DataFrame with DateType columns") {

    val df = spark.createDF(
      List(
        (1, Date.valueOf("2016-09-30")),
        (2, Date.valueOf("2016-12-14"))
      ), List(
        ("person_id", IntegerType, true),
        ("birth_date", DateType, true)
      )
    )

    df.show()

    df.printSchema()

  }

//  it("can't create a DateType column with Strings and errors out") {
//
//    val df = spark.createDF(
//      List(
//        (1, "2016-09-30"),
//        (2, "2016-12-14")
//      ), List(
//        ("person_id", IntegerType, true),
//        ("birth_date", DateType, true)
//      )
//    )
//
//    df.show()
//
//  }

  it("shows withTwoDaysEarlier in action") {
    val df = spark.createDF(
      List(
        (1, Date.valueOf("2016-09-30")),
        (2, Date.valueOf("2016-12-14"))
      ), List(
        ("person_id", IntegerType, true),
        ("birth_date", DateType, true)
      )
    ).transform(withTwoDaysEarlier("birth_date"))

    df.show()
  }

  it("adds a two_days_ago column") {

val df = spark.createDF(
  List(
    (1, Date.valueOf("2016-09-30"), Date.valueOf("2016-09-28")),
    (2, Date.valueOf("2016-12-14"), Date.valueOf("2016-12-12")),
    (3, null, null)
  ), List(
    ("person_id", IntegerType, true),
    ("birth_date", DateType, true),
    ("expected", DateType, true)
  )
).transform(withTwoDaysEarlier("birth_date"))

assertColumnEquality(df, "two_days_earlier", "expected")

  }

}
