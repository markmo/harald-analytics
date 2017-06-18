name := "harald-analytics"

version := "1.0"

scalaVersion := "2.11.8"

val akkaVersion = "2.3.15"
val akkaHttpVersion = "2.0.5"
val hadoopVersion = "2.7.1"
val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "com.github.nscala-time" %% "nscala-time" % "2.16.0",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-http-experimental" % akkaHttpVersion,
  "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaHttpVersion,
  "io.spray" %%  "spray-json" % "1.3.3",
  "net.cakesolutions" %% "scala-kafka-client" % "0.10.2.1"
    excludeAll ExclusionRule(organization = "com.typesafe.akka"),
  "net.cakesolutions" %% "scala-kafka-client-akka" % "0.10.2.1"
    excludeAll ExclusionRule(organization = "com.typesafe.akka"),
  "com.typesafe.play" % "play-json_2.11" % "2.5.12"
    excludeAll ExclusionRule(organization = "com.fasterxml.jackson.core"),
  "com.google.firebase" % "firebase-admin" % "4.1.1",
  "org.apache.spark" %% "spark-streaming" % sparkVersion
    excludeAll ExclusionRule(organization = "com.typesafe.akka"),
  "org.apache.bahir" %% "spark-streaming-akka" % sparkVersion
    excludeAll ExclusionRule(organization = "com.typesafe.akka"),
  "org.apache.spark" %% "spark-sql" % sparkVersion
    excludeAll ExclusionRule(organization = "com.typesafe.akka"),
  "log4j" % "log4j" % "1.2.17"
)

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

resolvers += "apache-snapshot" at "https://repository.apache.org/content/repositories/snapshots"

initialCommands in console := """
                                |import org.apache.spark._
                                |import org.apache.spark.streaming._
                                |import org.apache.spark.streaming.akka.{ ActorReceiver, AkkaUtils }
                                |import _root_.akka.actor.{ ActorSystem, Actor, Props }
                                |import com.typesafe.config.ConfigFactory
                                |""".stripMargin

fork in run := true
javaOptions in run ++= Seq(
  "-Dlog4j.debug=false",
  "-Dlog4j.configuration=log4j.properties")
outputStrategy := Some(StdoutOutput)
