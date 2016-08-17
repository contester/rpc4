import sbtprotobuf.{ProtobufPlugin=>PB}

seq(PB.protobufSettings: _*)

name := "rpc4"

scalaVersion := "2.10.3"

version := "0.1"

organization := "org.stingray.contester"

scalacOptions ++= Seq("-unchecked", "-deprecation")

javacOptions in Compile ++= Seq("-source", "1.6",  "-target", "1.7")

version in PB.protobufConfig := "2.5.0"

resolvers ++= Seq(
    "twitter.com" at "http://maven.twttr.com/",
    "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases",
    "scala tools" at "http://scala-tools.org/repo-releases/",
    "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/",
    "typesafe artefactory" at "http://typesafe.artifactoryonline.com/typesafe/repo",
    "stingr.net" at "http://stingr.net/maven"
)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.9.0.Final",
  "com.twitter" %% "finagle-core" % "6.10.0",
  "com.twitter" %% "util-core" % "6.10.0",
  "org.scalatest" %% "scalatest" % "1.9.2" % "test"
)
