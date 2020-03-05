package com.github.mrpowers.scalatest.example

class Person(firstName: String, lastName: String) {

  def fullName(): String = {
    firstName + " " + lastName
  }

}
