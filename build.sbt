name := "raikumock"

organization := "com.github.mvollebregt"

version := "0.3.9"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "nl.gideondk" %% "raiku" % "0.3.9"
)

resolvers ++= Seq("gideondk-repo" at "https://raw.github.com/gideondk/gideondk-mvn-repo/master")