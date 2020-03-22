resolvers += "Spark Packages Repo" at "http://dl.bintray.com/spark-packages/maven"

name := "scalatest-example"

version := "0.0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "MrPowers" % "spark-fast-tests" % "0.20.0-s_2.12" % "test"
libraryDependencies += "mrpowers" % "spark-daria" % "0.36.0-s_2.12"

// test suite settings
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
// Show runtime of tests
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
envVars in Test := Map("PROJECT_ENV" -> "test")

// JAR file settings

// don't include Scala in the JAR file
//assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

// Add the JAR file naming conventions described here: https://github.com/MrPowers/spark-style-guide#jar-files
// You can add the JAR file naming conventions by running the shell script
