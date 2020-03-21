package com.github.mrpowers.scalatest.example

object Config {

  var test: Map[String, String] = {
    Map(
      "dogPath" -> new java.io.File("src/test/resources/dogs.csv").getCanonicalPath
    )
  }

  var production: Map[String, String] = {
    Map(
      "dogPath" -> "/some/path/in/s3/"
    )
  }

  var environment = sys.env.getOrElse("PROJECT_ENV", "production")

  def get(key: String): String = {
    if (environment == "test") {
      test(key)
    } else {
      production(key)
    }
  }

}
