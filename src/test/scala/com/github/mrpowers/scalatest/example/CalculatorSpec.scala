package com.github.mrpowers.scalatest.example

import org.scalatest.FunSpec

class CalculatorSpec extends FunSpec {

  describe("addNumbers") {

    it("adds two numbers") {

      assert(Calculator.addNumbers(3, 4) === 7)

    }

  }

}