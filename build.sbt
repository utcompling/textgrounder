name := "TextGrounder"

version := "0.3.0"

organization := "OpenNLP"

scalaVersion := "2.9.1"

crossPaths := false

retrieveManaged := true

resolvers ++= Seq(
  "OpenNLP Maven Repository" at "http://opennlp.sourceforge.net/maven2"
//  Resolver for trove-scala source; nonexistent here yet
//  "repo.codahale.com" at "http://repo.codahale.com",
//  Resolver if you want to find stuff out of your local Maven cache
//  "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"
  )


libraryDependencies ++= Seq(
  "com.google.inject" % "guice" % "2.0",
  "com.google.guava" % "guava" % "r06",
  // Not needed; only if you run into error retrieving commons-cli
  // (happened to me in the past)
  // "commons-cli" % "commons-cli" % "1.2",
  "org.jdom" % "jdom" % "1.1",
  "org.xerial" % "sqlite-jdbc" % "3.6.20",
  "org.apache.opennlp" % "opennlp-maxent" % "3.0.1-incubating",
  "org.apache.opennlp" % "opennlp-tools" % "1.5.1-incubating",
  "org.clapper" %% "argot" % "0.3.5"
//  Find repository for trove-scala; currently stored unmanaged
//  "com.codahale" % "trove-scala_2.9.1" % "0.0.1-SNAPSHOT"
  )

// append several options to the list of options passed to the Java compiler
javacOptions ++= Seq("-Xlint")

// append -deprecation to the options passed to the Scala compiler
scalacOptions ++= Seq("-deprecation", "-Xlint")
