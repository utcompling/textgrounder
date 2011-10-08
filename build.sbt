name := "TextGrounder"

version := "0.2"

organization := "OpenNLP"

scalaVersion := "2.9.1"

crossPaths := false

retrieveManaged := true

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % "2.0",
  "com.google.guava" % "guava" % "r06",
  "commons-cli" % "commons-cli" % "1.2",
  "org.jdom" % "jdom" % "1.1",
  "org.xerial" % "sqlite-jdbc" % "3.6.20",
  "opennlp" % "maxent" % "3.0.0",
  "opennlp" % "tools" % "1.5.0",
  "org.clapper" %% "argot" % "0.3.5"
  )

// append several options to the list of options passed to the Java compiler
javacOptions ++= Seq("-Xlint")

// append -deprecation to the options passed to the Scala compiler
scalacOptions ++= Seq("-deprecation", "-Xlint")

// defaultExcludes ~= (filter => filter || "*~")

// seq(sbtassembly.Plugin.assemblySettings: _*)

// jarName in Assembly := "textgrounder-assembly.jar"

