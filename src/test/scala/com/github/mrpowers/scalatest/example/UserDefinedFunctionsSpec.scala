package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec
import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import com.github.mrpowers.spark.fast.tests.ColumnComparer
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

class UserDefinedFunctionsSpec
    extends FunSpec
    with SparkSessionTestWrapper
    with ColumnComparer {

  describe("lowerRemoveAllWhitespace") {

//    it("prints out the function results for people that are still learning") {
//      val df = spark.createDF(
//        List(
//          ("  HI THERE     "),
//          (" GivE mE PresenTS     ")
//        ), List(
//          ("aaa", StringType, true)
//        )
//      ).withColumn(
//        "clean_aaa",
//        UserDefinedFunctions.lowerRemoveAllWhitespace(col("aaa"))
//      )
//
//      df.show(truncate = false)
//    }
//
//    it("downcases the letters and removes all whitespace") {
//      val df = spark.createDF(
//        List(
//          ("  HI THERE     ", "hithere"),
//          (" GivE mE PresenTS     ", "givemepresents")
//        ), List(
//          ("aaa", StringType, true),
//          ("expected", StringType, true)
//        )
//      ).withColumn(
//        "clean_aaa",
//        UserDefinedFunctions.lowerRemoveAllWhitespace(col("aaa"))
//      )
//
//      assertColumnEquality(df, "clean_aaa", "expected")
//    }

//  it("blows up with null input") {
//    val df = spark.createDF(
//      List(
//        ("  HI THERE     "),
//        (" GivE mE PresenTS     "),
//        (null)
//      ), List(
//        ("aaa", StringType, true)
//      )
//    ).withColumn(
//      "clean_aaa",
//      UserDefinedFunctions.lowerRemoveAllWhitespace(col("aaa"))
//    )
//
//    df.show()
//  }

it("verifies the code blows up with null input") {
  val df = spark.createDF(
    List(
      ("  HI THERE     "),
      (" GivE mE PresenTS     "),
      (null)
    ), List(
      ("aaa", StringType, true)
    )
  ).withColumn(
    "clean_aaa",
    UserDefinedFunctions.lowerRemoveAllWhitespace(col("aaa"))
  )

  assertThrows[org.apache.spark.SparkException] {
    df.show()
  }
}

  }

}
