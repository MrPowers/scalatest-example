package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import org.apache.spark.sql.types._
import OrganizingTests._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._

class OrganizingTestsSpec extends FunSpec with SparkSessionTestWrapper with ColumnComparer {

  describe("analyzeTextFeeling") {
    it("returns positive if the text has good vibes") {
      val text = "This product makes me happy"
      assert(analyzeTextFeeling(text) === "positive")
    }

    it("returns negative if the text doesn't have good vibes") {
      val text = "I don't like this product"
      assert(analyzeTextFeeling(text) === "negative")
    }
  }

  describe("bad organization") {
    it("returns positive if the text has good vibes") {
      val df = spark.createDF(
        List(
          ("This product makes me happy", "positive")
        ), List(
          ("customer_review", StringType, true),
          ("expected", StringType, true)
        )
      ).transform(withAnalyzedFeeling("customer_review"))

      assertColumnEquality(df, "analyzed_feeling", "expected")
    }

    it("returns negative if the text doesn't have good vibes") {
      val df = spark.createDF(
        List(
          ("I don't like this product", "negative")
        ), List(
          ("customer_review", StringType, true),
          ("expected", StringType, true)
        )
      ).transform(withAnalyzedFeeling("customer_review"))

      assertColumnEquality(df, "analyzed_feeling", "expected")
    }

    it("handles the null case") {
      val df = spark.createDF(
        List(
          (null, null)
        ), List(
          ("customer_review", StringType, true),
          ("expected", StringType, true)
        )
      ).transform(withAnalyzedFeeling("customer_review"))

      assertColumnEquality(df, "analyzed_feeling", "expected")
    }
  }

  describe("good organization") {
    it("analyzes the sentiment of the customer review") {
      val df = spark.createDF(
        List(
          // returns positive if the text has good vibes
          ("This product makes me happy", "positive"),
          // returns negative if the text doesn't have good vibes
          ("I don't like this product", "negative"),
          // handles the null case
          (null, null)
        ), List(
          ("customer_review", StringType, true),
          ("expected", StringType, true)
        )
      ).transform(withAnalyzedFeeling("customer_review"))

      assertColumnEquality(df, "analyzed_feeling", "expected")
    }
  }

}
