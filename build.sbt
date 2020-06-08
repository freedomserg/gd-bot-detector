name := "ad-events-processor"

version := "0.1"

scalaVersion := "2.12.11"
val sparkVersion = "2.4.5"
val circeVersion = "0.12.3"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
//  "org.apache.spark" %% "spark-streaming" % sparkVersion % "test",
//  "org.apache.spark" %% "spark-streaming" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "net.debasishg" %% "redisclient" % "3.30",
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
  "io.circe" %% "circe-core" % circeVersion,
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
//  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
//  "org.mockito"  % "mockito-core" % "1.9.5" % "test",
  "com.holdenkarau" %% "spark-testing-base" % "2.4.5_0.14.0" % "test",
  "com.typesafe.akka" %% "akka-http"   % "10.1.12" % "test",
  "com.typesafe.akka" %% "akka-stream" % "2.6.5" % "test"
)

//libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion artifacts(Artifact("spark-streaming","tests"))

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false


