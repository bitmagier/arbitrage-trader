name := "arbitrage-trader"

version := "0.9.0-SNAPSHOT"

scalaVersion := "2.12.11"


libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.30"
libraryDependencies += "ch.qos.logback"% "logback-classic" % "1.2.3"
libraryDependencies += "ch.qos.logback"% "logback-core" % "1.2.3"

val AkkaVersion = "2.6.8"
val AkkaHttpVersion = "10.1.12"
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % AkkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % AkkaHttpVersion
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % AkkaVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test
libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test
