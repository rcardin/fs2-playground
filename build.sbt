ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "fs2-playground"
  )

val Fs2Version = "3.2.4"

libraryDependencies += "co.fs2" %% "fs2-core" % Fs2Version
