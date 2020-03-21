package com.github.mrpowers.scalatest.example

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.flatspec.AnyFlatSpec

class AnyFlatExampleSpec
    extends AnyFlatSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  "a" should "b" in {
    println("works")
  }

  it should "have size 0" in {
    assert(Set.empty.size === 0)
  }

  it should "compare two DataFrames" in {
    val df1 = Seq((2), (6), (5)).toDF("num")
    val df2 = Seq((2), (6), (5)).toDF("num")
    assertSmallDataFrameEquality(df1, df2)
  }

}
