name := "arbitrage-trader"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.12.11"

val akkaVersion = "2.6.8"
val akkaHttpVersion = "10.1.12"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http" % akkaHttpVersion
//libraryDependencies += "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion

