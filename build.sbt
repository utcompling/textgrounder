name := "TextGrounder"

version := "0.3.0"

organization := "OpenNLP"

scalaVersion := "2.9.1"

crossPaths := false

retrieveManaged := true

resolvers += "OpenNLP Maven Repository" at "http://opennlp.sourceforge.net/maven2"

libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % "2.0",
  "com.google.guava" % "guava" % "r06",
  "commons-cli" % "commons-cli" % "1.2",
  "org.jdom" % "jdom" % "1.1",
  "org.xerial" % "sqlite-jdbc" % "3.6.20",
  "org.apache.opennlp" % "opennlp-maxent" % "3.0.1-incubating",
  "org.apache.opennlp" % "opennlp-tools" % "1.5.1-incubating",
  "org.clapper" %% "argot" % "0.3.5"
  )

// append several options to the list of options passed to the Java compiler
javacOptions ++= Seq("-Xlint")

// append -deprecation to the options passed to the Scala compiler
scalacOptions ++= Seq("-deprecation", "-Xlint")


