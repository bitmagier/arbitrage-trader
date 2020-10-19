name := "arbitrage-trader"

version := "0.9.3-SNAPSHOT"

scalaVersion := "2.12.12"


val AkkaVersion = "2.6.10"
val AkkaHttpVersion = "10.1.12"

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % AkkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % AkkaHttpVersion
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % AkkaVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test
libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test
