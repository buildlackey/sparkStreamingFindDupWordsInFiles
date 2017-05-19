name := "streaming-example"

version := "1.0"

scalaVersion :=  "2.11.6"

// make sure scalaVersion  is respected.
// see: http://stackoverflow.com/questions/22551430/in-sbt-0-13-does-scalaversion-still-control-the-version-of-scala-used-for-compi
dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.1"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "commons-io" % "commons-io" % "2.5"

libraryDependencies += "com.typesafe" % "config" % "1.3.1"
