name := "redis-scala"

organization := "com.impactua"

val revision = sys.env.getOrElse("TRAVIS_BUILD_NUMBER", "0-SNAPSHOT")

version := s"""1.3.$revision"""

scalaVersion := "2.11.8"

crossScalaVersions := Seq("2.10.4", "2.11.8")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false

bintrayReleaseOnPublish := false

bintrayPackage := name.value

bintrayOrganization in bintray := Some("sergkh")

concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.10.6.Final",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
  "com.storm-enroute" %% "scalameter" % "0.7" % "test"
)
