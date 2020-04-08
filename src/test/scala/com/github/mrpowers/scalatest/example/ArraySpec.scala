package com.github.mrpowers.scalatest.example

import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, ColumnComparer}
import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class ArraySpec extends FunSpec with SparkSessionTestWrapper with DataFrameComparer with ColumnComparer {

  it("demonstrates how to manually create a DataFrame with an ArrayType column") {
    val df = spark.createDF(
      List(
        ("kobe", Array("black mamba", "the dagger")),
        ("shaq", Array("disel", "shaq fu"))
      ), List(
        ("first_name", StringType, true),
        ("nicknames", ArrayType(StringType, true), true)
      )
    )

    df.show(false)
  }

  it("performs a simple array test") {
    val df = spark.createDF(
      List(
        (Array(8, 23), Array(8, 23)),
        (Array(2, 3), Array(2, 3))
      ), List(
        ("numbers1", ArrayType(IntegerType, true), true),
        ("numbers2", ArrayType(IntegerType, true), true)
      )
    )

    assertColumnEquality(df, "numbers1", "numbers2")
  }

//  it("prints a nice error message when the arrays are not equal") {
//    val df = spark.createDF(
//      List(
//        (Array("have", "fun"), Array("have", "fun")),
//        (Array("like", "colombia"), Array("like", "brazil"))
//      ), List(
//        ("words1", ArrayType(StringType, true), true),
//        ("words2", ArrayType(StringType, true), true)
//      )
//    )
//
//    assertColumnEquality(df, "words1", "words2")
//  }

  it("performs Array DataFrame equality operations") {
val df = spark.createDF(
  List(
    ("a", "b", 1),
    ("a", "b", 2),
    ("a", "b", 3),
    ("z", "b", 4),
    ("a", "x", 5)
  ), List(
    ("letter1", StringType, true),
    ("letter2", StringType, true),
    ("number1", IntegerType, true)
  )
)

val collapsedDF = df
  .groupBy("letter1", "letter2")
  .agg(collect_list("number1") as "number1s")

val expectedDF = spark.createDF(
  List(
    ("a", "b", Array(1, 2, 3)),
    ("z", "b", Array(4)),
    ("a", "x", Array(5))
  ), List(
    ("letter1", StringType, true),
    ("letter2", StringType, true),
    ("number1s", ArrayType(IntegerType, true), true)
  )
)

assertSmallDataFrameEquality(collapsedDF, expectedDF, orderedComparison = false)
  }

}
