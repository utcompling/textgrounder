import AssemblyKeys._ // put this at the top of the file

name := "TextGrounder"

version := "0.3.0"

organization := "OpenNLP"

scalaVersion := "2.9.1"

crossPaths := false

retrieveManaged := true

resolvers ++= Seq(
  "OpenNLP Maven Repository" at "http://opennlp.sourceforge.net/maven2",
  "repo.codahale.com" at "http://repo.codahale.com"
//  Resolver for trove-scala source; nonexistent here yet
//  "repo.codahale.com" at "http://repo.codahale.com",
//  Resolver if you want to find stuff out of your local Maven cache
//  "Local Maven Repository" at "file://"+Path.userHome+"/.m2/repository"
  )


libraryDependencies ++= Seq(
  "com.google.guava" % "guava" % "10.0.1",
  "org.jdom" % "jdom" % "1.1",
  "org.apache.commons" % "commons-lang3" % "3.1",
  "org.apache.commons" % "commons-compress" % "1.3",
  // The following don't appear to be used currently.
  // "com.google.inject" % "guice" % "2.0",
  // "commons-cli" % "commons-cli" % "1.2",
  // "org.xerial" % "sqlite-jdbc" % "3.6.20",
  // "org.apache.opennlp" % "opennlp-maxent" % "3.0.1-incubating",
  "org.apache.opennlp" % "opennlp-tools" % "1.5.1-incubating",
  // The use of %% instead of % causes the Scala version to get appended,
  // i.e. it's equivalent to the use of single % with "argot_2.9.1".
  // This is for Scala-specific dependencies.
  // Remove this for the moment because there's a modified version (styled
  // as version "0.3.5-benwing") in the unmanaged lib/. (Fuck me, our
  // unmanaged junto.jar also includes a copy of Argot 0.3.5, so we will have
  // class path problems with a newer managed version of Argot until we
  // remove this!)
  // "org.clapper" %% "argot" % "0.3.5",
  "org.apache.hadoop" % "hadoop-core" % "0.20.205.0",
  // Necessary because of a stupid bug in the Maven POM for Hadoop, which
  // leaves this out.  NOTE: Supposedly Hadoop originally used version 1.5.2.
  // If we get failures, set the version back to that.
  "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.1",
  // The following needed for Scoobi
  "javassist" % "javassist" % "3.12.1.GA",
  // Trove
  "net.sf.trove4j" % "trove4j" % "3.0.2",
//  Find repository for trove-scala; currently stored unmanaged
//  "com.codahale" % "trove-scala_2.9.1" % "0.0.1-SNAPSHOT"
    "com.codahale" % "jerkson_2.9.1" % "0.5.0"  
  )

// turn on all warnings in Java code
javacOptions ++= Seq("-Xlint")

// turn on all Scala warnings; also turn on deprecation warnings
scalacOptions ++= Seq("-deprecation", "-Xlint", "-unchecked")

seq(assemblySettings: _*)

test in assembly := {}

//excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
//  cp filter {x => Seq("jasper-compiler-5.5.12.jar", "jasper-runtime-5.5.12.jar", "commons-beanutils-1.7.0.jar", "servlet-api-2.5-20081211.jar") contains x.data.getName }
//}

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case x => {
      val oldstrat = old(x)
      if (oldstrat == MergeStrategy.deduplicate) MergeStrategy.first
      else oldstrat
    }
  }
}

jarName in assembly := "textgrounder-assembly.jar"
