package com.github.mrpowers.scalatest.example

import org.scalatest.FreeSpec

class PersonSpec extends FreeSpec {

  "fullName" - {

    "returns the first name and last name concatenated" in {

      val lilXan = new Person("Lil", "Xan")
      assert(lilXan.fullName() === "Lil Xan")

    }

  }

}
