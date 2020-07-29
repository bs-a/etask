name := "etask"

version := "0.1"

scalaVersion := "2.13.3"

lazy val akkaVersion = "2.5.31"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "2.0.1",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "2.0.1"

)
