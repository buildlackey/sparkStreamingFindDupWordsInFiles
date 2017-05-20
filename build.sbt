name := "streaming-example"

version := "1.0"

scalaVersion :=  "2.11.6"

// Make sure scalaVersion  is respected.
// See: http://stackoverflow.com/questions/22551430/in-sbt-0-13-does-scalaversion-still-control-the-version-of-scala-used-for-compi
dependencyOverrides += "org.scala-lang" % "scala-library" % scalaVersion.value

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.1.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "commons-io" % "commons-io" % "2.5"
libraryDependencies += "com.typesafe" % "config" % "1.3.1"

// Test dependencies
libraryDependencies += "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test"



// Settings required for unit tests that use spark-testing-base
// See: https://github.com/holdenk/spark-testing-base
fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
parallelExecution in Test := false


