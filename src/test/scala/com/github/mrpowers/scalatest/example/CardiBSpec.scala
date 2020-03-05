package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec

class CardiBSpec extends FunSpec {

  describe("realName") {

    it("returns her birth name") {
      assert(CardiB.realName() === "Belcalis Almanzar")
    }

  }

  describe("iLike") {

    it("works with a single argument") {
      assert(CardiB.iLike("dollars") === "I like dollars")
    }

    it("works with multiple arguments") {
      assert(CardiB.iLike("dollars", "diamonds") === "I like dollars, diamonds")
    }

    it("throws an error if an integer argument is supplied") {
      assertThrows[java.lang.IllegalArgumentException]{
        CardiB.iLike()
      }
    }

    it("does not compile with integer arguments") {
      assertDoesNotCompile("""CardiB.iLike(1, 2, 3)""")
    }

  }

}
