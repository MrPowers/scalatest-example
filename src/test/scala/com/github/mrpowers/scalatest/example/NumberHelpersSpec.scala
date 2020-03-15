package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import com.github.mrpowers.spark.fast.tests.ColumnComparer

class NumberHelpersSpec extends FunSpec with SparkSessionTestWrapper with ColumnComparer {

  import spark.implicits._

  describe("isEven") {

    it("returns true for even numbers") {

      val df = Seq(
        (2, true),
        (6, true),
        (5, false)
      ).toDF("num", "expected")

      val resDF = df.transform(NumberHelpers.isEven("num", "is_even"))

      assertColumnEquality(resDF, "expected", "is_even")

    }

  }

}
