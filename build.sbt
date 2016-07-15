name := "redis-scala"

organization := "com.impactua"

val revision = sys.env.getOrElse("TRAVIS_BUILD_NUMBER", "0-SNAPSHOT")

version := s"""1.3.$revision"""

scalaVersion := "2.11.6"

crossScalaVersions := Seq("2.10.4", "2.11.6")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false

bintrayReleaseOnPublish := false

bintrayPackage := name.value

bintrayOrganization in bintray := Some("sergkh")

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.10.6.Final",
  "junit" % "junit" % "4.12" % "test",
  "org.scalatest" % "scalatest_2.11" % "3.0.0-M15",
  "com.storm-enroute" %% "scalameter" % "0.7" % "test"
)
