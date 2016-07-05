name := "redis-scala"

organization := "com.impactua"

version := "1.2.10"

scalaVersion := "2.11.5"

crossScalaVersions := Seq("2.10.4", "2.11.5")

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

publishMavenStyle := true
publishArtifact := true
publishArtifact in Test := false

bintrayReleaseOnPublish := false

bintrayPackage := name.value

bintrayOrganization in bintray := Some("sergkh")

libraryDependencies ++= Seq(
  "io.netty" % "netty" % "3.10.4.Final",
  "junit" % "junit" % "4.12" % "test",
  "com.storm-enroute" %% "scalameter" % "0.7" % "test"
)