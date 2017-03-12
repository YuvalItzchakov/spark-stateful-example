scalaVersion in ThisBuild := "2.11.8"

name := "Spark Stateful Streaming"

libraryDependencies ++= Seq(
  "com.github.melrief" %% "pureconfig" % "0.6.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "io.argonaut" %% "argonaut" % "6.1"
)