name := "arbitrage-trader"

version := "0.9.4-SNAPSHOT"

scalaVersion := "2.12.12"


val AkkaVersion = "2.6.10"
val AkkaHttpVersion = "10.2.1"

libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"

libraryDependencies += "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % AkkaHttpVersion
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % AkkaVersion

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"
libraryDependencies += "com.typesafe.akka" %% "akka-actor-testkit-typed" % AkkaVersion % Test
//libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test
