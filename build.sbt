import AssemblyKeys._ // for sbt-assembly

name := "TextGrounder"

version := "0.3.0"

organization := "OpenNLP"

scalaVersion := "2.9.2"

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

// The following needed for Scoobi
resolvers ++= Seq("Cloudera Maven Repository" at "https://repository.cloudera.com/content/repositories/releases/",
              "Packaged Avro" at "http://nicta.github.com/scoobi/releases/")

// The following needed for Scoobi snapshots
resolvers += "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots"

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
  // 
  // The following is the old way we got Hadoop added.  Out of date, has lots
  // of problems.  Now it's included as a dependency of Scoobi. 
  // "org.apache.hadoop" % "hadoop-core" % "0.20.205.0",
  // This was necessary because of a stupid bug in the Maven POM for Hadoop
  // (the "old way") above, which leaves this out. (Supposedly Hadoop
  // originally used version 1.5.2, but version 1.9.1 doesn't seem to cause
  // problems.) But not needed at all if we don't use that Hadoop POM.
  // "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.1",
  //
  // Currently log4j is provided automatically by something (probably Scoobi).
  // If this fails to be the case at some point, we can use the following:
  // "log4j" % "log4j" % "1.2.17",
  //
  // Trove
  "net.sf.trove4j" % "trove4j" % "3.0.2",
  //
  // Scoobi
  "com.nicta" % "scoobi_2.9.2" % "0.4.0",
  // "provided" if we use Scoobi's package-hadoop instead of sbt-assembly.
  // This is another way of building an assembly for Hadoop that includes all
  // the dependent libraries into the JAR file.  To do that, we have to move
  // the file 'scoobi.scala' in 'project/' into 'project/project/', and
  // comment out all the lines related to sbt-assembly (the import at the top,
  // and everything below starting with 'seq(assemblySettings ...)'),
  // because of stupid incompatibilities between the two.  Then we can use
  // 'textgrounder build package-hadoop' instead of
  // 'textgrounder build assembly'.
  // "com.nicta" % "scoobi_2.9.2" % "0.4.0" % "provided",
  // A newer version that fixes a bug handling empty intermediate checkpoints
  // in Scoobi (among other things), but leads to compile errors that I don't
  // know how to fix.
  //  "com.nicta" % "scoobi_2.9.2" % "0.5.0-SNAPSHOT" % "provided",
  // The following needed for Scoobi 0.1, but evidently not any more.
  // "javassist" % "javassist" % "3.12.1.GA",
  //
  // Find repository for trove-scala; currently stored unmanaged
  // "com.codahale" % "trove-scala_2.9.1" % "0.0.2-SNAPSHOT"
  // Jerkson - a better library for processing JSON, although still in
  // development
  "com.codahale" % "jerkson_2.9.1" % "0.5.0"
  )

// turn on all warnings in Java code
javacOptions ++= Seq("-Xlint")

// turn on all Scala warnings; also turn on deprecation warnings.
// "-Ydependent-method-types" is suggested by the Scoobi 0.4 documentation.
// Who knows what it does?  Evidently it's recently (end of 2011) been made
// on by default, although I assume this applies only to Scala 2.10.
scalacOptions ++= Seq("-Ydependent-method-types", "-deprecation", "-Xlint", "-unchecked")

seq(assemblySettings: _*)

test in assembly := {}

//excludedJars in assembly <<= (fullClasspath in assembly) map { cp => 
//  cp filter {x => Seq("jasper-compiler-5.5.12.jar", "jasper-runtime-5.5.12.jar", "commons-beanutils-1.7.0.jar", "servlet-api-2.5-20081211.jar") contains x.data.getName }
//}

// FUCK ME TO (JAR) HELL! This is an awful hack. Boys and girls, repeat after
// me: say "fragile library problem" and "Java sucks rocks compared with C#".
// Now repeat 100 times.
//
// Here the problem is that, as a program increases in size and includes
// dependencies from various sources, each with their own sub-dependencies,
// you'll inevitably end up with different versions of the same library as
// sub-dependencies of different dependencies.  This is the infamous "fragile
// library problem" (aka DLL hell, JAR hell, etc.). Java has no solution to
// this problem. C# does. (As with 100 other nasty Java problems that don't
// exist in C#.)
//
// On top of this, SBT makes things even worse by not even providing a way
// of automatically picking the most recent library.  In fact, it doesn't
// provide any solution at all that doesn't require you to write your own
// code (see below) -- a horrendous solution typical of packages written by
// programmers who are obsessed with the mantra of "customizability" but
// have no sense of proper design, no knowledge of how to write user
// interfaces, and no skill in creating understandable documentation.
// The "solution" below arbitrarily picks the first library version found.
// Is this newer or older?  Will it cause weird random breakage?  Who knows?

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
