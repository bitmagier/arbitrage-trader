name := "arbitrage-trader"

version := "0.5.3-SNAPSHOT"

scalaVersion := "2.12.11"


libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.30"
libraryDependencies += "ch.qos.logback"% "logback-classic" % "1.2.3"
libraryDependencies += "ch.qos.logback"% "logback-core" % "1.2.3"

val akkaVersion = "2.6.8"
val akkaHttpVersion = "10.1.12"
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % akkaVersion
libraryDependencies += "com.typesafe.akka" %% "akka-http-core" % akkaHttpVersion
libraryDependencies += "io.spray" %% "spray-json" % "1.3.5"
libraryDependencies += "com.typesafe.akka" %% "akka-stream" % akkaVersion
