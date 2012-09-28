This file contains documentation on the Poligrounder application, including
an overview as well as configuration and build instructions. 

Poligrounder is open-source software, stored here:

https://github.com/benwing/poligrounder

Poligrounder is a fork of TextGrounder.  TextGrounder is a system for
document-level and toponym-level geolocation, whose site is as follows:

https://github.com/utcompling/textgrounder

Poligrounder instead seeks to find relevant features in a collection of tweets
that are distinguished across the occurrence of an event (e.g. an announcement
of a new policy by a president, a presidental debate, etc.).

============================
Introduction to Poligrounder
============================

Poligrounder consists of a number of applications, all of which are written
in Scala.  Scala is a hybrid object-oriented/functional language built on
top of the Java JVM.  This means that Scala code compiles to Java bytecode
and can be run just like any Java app, and that Scala and Java code can be
freely intermixed.  In many ways, Scala is a "better Java" that provides
numerous features not present in Java (although many are in C#), and makes
coding Java-type apps easier, faster and less painful.

The applications are as follows:

1. opennlp.textgrounder.preprocess.ParseTweets: This parses tweets in various
   ways, optionally filtering and/or grouping them by user, time, etc.
   Numerous options are provided.
2. opennlp.textgrounder.preprocess.FindPolitical: This locates "ideological
   users" and identifies their ideology, based on an initial set of known
   ideological users (e.g. a set of political office-holders and their
   party identifications).  It also looks for ideologically-based features,
   e.g. hashtags, and provides an ideology for each.
3. opennlp.textgrounder.poligrounder.PoligrounderApp: Does two-way
   before/after or four-way before/after, liberal/conservative comparisons
   in a corpus, looking for words that differ across subcorpora and
   sorting by log-likelihood.

============
Requirements
============

* Version 1.6 of the Java 2 SDK (http://java.sun.com)
* git, for downloading the code of Poligrounder and Scoobi
* A modified version of Scoobi (see below)
* Version 0.12 of SBT (used for building Scala apps, particularly Scoobi)
* Hadoop 0.20.2, particularly the Cloudera cdh3 build
* Appropriate data files, consisting of tweets (see below)


===================
Building the system
===================

The following describes how to quickly get Poligrounder up and running and to
test that everything is in place and working.

I. Java and Git

1. Make sure you have Java installed.  Poligrounder is developed and tested
   on Java 6, but it might work on Java 5 (definitely not earlier).  Scoobi
   and SBT also both depend on Java, preferably Java 6.

2. Set your JAVA_HOME environment variable to the top level of the Java
   installation tree.  On Mac OS X it's probably /Library/Java/Home.  On
   the University of Maryland (UMD) CLIP machines running Red Hat Enterprise
   Linux, it's probably /usr/lib/jvm/jre-1.6.0-sun.x86_64.

3. Make sure you have Git installed. 


II. Download SBT

1. Download SBT version 0.12 or later.  The download page is as follows:

   http://www.scala-sbt.org/download.html

   Near the bottom, in the "by hand" section, there is a link to 'sbt-launch.jar'.
   Currently, the following link works, but might move:

   http://typesafe.artifactoryonline.com/typesafe/ivy-releases/org.scala-sbt/sbt-launch/0.12.0/sbt-launch.jar

2. Save the file "sbt-launch.jar" to your bin directory.  It is best to rename
   this to reflect the appropriate version, since SBT-based apps tend to be
   rather sensitive to the particular version of SBT.  E.g. in this case,
   rename the file to 'sbt-launch-0.12.0.jar'.

3. Create a script to run SBT.  Call it 'sbt' and put it in your bin directory,
   where you saved 'sbt-launch.jar'.  It should look as follows:

   ---------------------------------- cut ------------------------------------ 
   #!/bin/sh

   java -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=512m -Xmx2048M -Xss2M -jar `dirname $0`/sbt-launch-0.12.0.jar "$@"
   ---------------------------------- cut ------------------------------------ 

4. Make your script executable, and make sure it's in your PATH.


III. Download and build Ben's modified Scoobi

This app uses a patched version of Scoobi, which is a toolkit for building
higher-level Hadoop-based apps using Scala.

1. Download the patched version of Scoobi.

   In some work directory, execute the following commands:

   $ git clone https://github.com/benwing/scoobi
   $ cd scoobi
   $ git checkout benwing-latest-cdh3

2. Build Scoobi.

   In the same top-level Scoobi directory, execute the following:

   $ sbt publish-local

   This uses the SBT script you created above.  It should download a bunch
   of libraries into ~/.ivy2 (including the Scala compiler and runtime
   libraries as well as Hadoop 0.20.2 Cloudera cdh3 libraries, among others),
   and then compile Scoobi, and eventually "publish" the built JAR files
   under ~/.ivy2/local.


IV. Download and build Poligrounder

1. Download Poligrounder.

   In some work directory, execute the following:

   $ git clone https://github.com/benwing/poligrounder
   $ cd poligrounder
   $ git checkout poligrounder

2. Set up environment variables and paths.

   Set POLIGROUNDER_DIR to the top-level directory of where Poligrounder
   is located.  Make sure $POLIGROUNDER_DIR/bin is in your PATH, so the
   scripts in the 'bin' directory get found automatically.

3. Build Poligrounder.

   Execute the following:

   $ poligrounder build assembly

   This should be executable from anywhere; it uses a script in
   $POLIGROUNDER_DIR/bin and relies on the setting in POLIGROUNDER_DIR
   to know where the source code is.  There is a copy of SBT 0.12.0
   located inside of Poligrounder, and everything after 'poligrounder build'
   actually gets passed directly to SBT.  In this case, the 'assembly'
   directive causes a bunch more JAR's to get downloaded, and then
   Poligrounder gets compiled, and eventually a self-contained "assembly"
   or "fat JAR" gets built containing all the JAR's necessary to run
   Poligrounder under Hadoop.

   Note that for local testing, it's not actually necessary to build an
   assembly.


V. Get the data

Poligrounder works with tweets.  Ben Wing has a very large collection of
tweets that have been downloaded over the last year or so, but only some
of the collection is currently located in UMD machines (this is because
there isn't sufficient space for everything).

The UMD data is in /fs/clip-political.  The tweets in particular are under
/fs/clip-political/corpora/twitter-pull.  Under this directory, the
'spritzer' directory contains part of the collection of tweets downloaded
using the Twitter "spritzer" mechanism.


VI. Test locally

The first thing is to do a test in Hadoop non-distributed mode.  This
doesn't require that you have an installation of Hadoop or are running on
a Hadoop job tracker machine, and hence is a good way of testing that
everything works.

A simple test corpus is the 'input-naleo-jun-21' corpus, containing about
3,300 tweets, located under /fs/clip-political/corpora.  Hence, you
should be able to run the following:

$ poligrounder --verbose --hadoop-nondist run opennlp.textgrounder.preprocess.ParseTweets /fs/clip-political/corpora/input-naleo-jun-21 output-naleo

(Note that '--verbose' isn't strictly necessary but is useful in seeing
exactly what commands are being executed.)

This should run ParseTweets in Hadoop non-distributed mode and store its
output into the 'output-naleo' directory, which must not already exist.
The resulting directory should look something like this:

---------------------------------- cut ------------------------------------ 
total 3200
-rw-r--r--  1 benwing  staff    12600 Sep 27 20:18 .input-naleo-jun-21-ch0out0-r-00000-training-unigram-counts-tweets.txt.crc
-rw-r--r--  1 benwing  staff       12 Sep 27 20:18 .input-naleo-jun-21-training-unigram-counts-tweets-schema.txt.crc
-rwxrwxrwx  1 benwing  staff  1611540 Sep 27 20:18 input-naleo-jun-21-ch0out0-r-00000-training-unigram-counts-tweets.txt*
-rwxrwxrwx  1 benwing  staff      330 Sep 27 20:18 input-naleo-jun-21-training-unigram-counts-tweets-schema.txt*
---------------------------------- cut ------------------------------------ 

The file 'input-naleo-jun-21-ch0out0-r-00000-training-unigram-counts-tweets.txt'
contains the result of parsing the tweets in the input corpus.  In this case,
it should contain 3,340 lines (one per tweet), each one containing information
about a tweet.  The format is a simple tab-separated database.  The following
is a sample line:

---------------------------------- cut ------------------------------------ 
NALEO   215752394993180672      file:/Users/benwing/devel/pg-exp/input-naleo-jun-21/naleo.tweets.2012-06-21.0522.bz2    1340274354000   1340274354000   1340274354000           3094    1620    en      1       MittRomney:1 MPRnews:1 BarackObama:1            NALEO:2 Election2012:1 latism:1 http%3A//minnesota.publicradio.org/features/npr.php?id=155457970:1      .@MPRNews references #NALEO and discusses #Election2012, @BarackObama, and @MittRomney http://t.co/iiV2FAu9 #NALEO #latism      ,:2 #naleo:2 and:2 #election2012:1 @mprnews:1 .:1 @mittromney:1 references:1 #latism:1 discusses:1 @barackobama:1
---------------------------------- cut ------------------------------------ 

See below for more information about the output format.


VII. Test under Hadoop

To run under Hadoop, you will need to do the following steps, in general:

1. Make sure you have a self-contained JAR assembly built (see above).
2. Request machines, for running Hadoop, if necessary.
3. Log into the Hadoop job tracker.
4. Copy your data into HDFS (the Hadoop Distributed File System), unless
   your data is stored on a local file system that is visible to *all*
   Hadoop nodes (the job tracker and all task trackers).
5. Submit the job to Hadoop.
6. Copy your data out of HDFS.


------ Running under Bespin ------

The Bespin cluster at UMD is a Hadoop cluster of 16 machines, each with
8 cores.  The name node is 'bespinsub00.umiacs.umd.edu'.  It's not necessary
to request any machines, but data must be copied into HDFS, since the
/fs/clip-political file system is not visible to the task nodes.


------ Running under TACC Longhorn ------

TACC is a supercomputer center at the University of Texas.  The Longhorn
cluster is where Ben Wing's tweet data is primarily stored.  This cluster
has about 256 8-core machines, of which 48 are available for running
Hadoop.  In order to use Hadoop, a subset of these machines must be requested
for a period of time, up to a maximum of 24 hours.  Once these machines are
available, one is made the job tracker and name node.  Hadoop servers are
started on this machine and an HDFS file system formatted.  This means that
data cannot be stored long-term in HDFS.  All file systems are visible on
all machines, so there is no need to copy data into HDFS, although it
must be copied out.

------ Copy data into HDFS ------

If it is necessary to copy data into HDFS, use 'hadoop fs -put' to copy
a directory tree into HDFS, which can then be accessed using a relative
URL.  If data is to be read from a local file system, it will need to be
accessed using an absolute URL with the 'file:///' prefix, e.g. on Longhorn:

file:///scratch/01683/benwing/corpora/twitter-pull/all-spritzer

------ Submit to Hadoop ------

The only difference from running locally (in non-distributed mode) is that
the '--hadoop' option is given instead of '--hadoop-nondist', e.g.:

$ poligrounder --verbose --hadoop run opennlp.textgrounder.preprocess.ParseTweets input-naleo-jun-21 output-naleo

Note, however, that in this case, both input and output directories are in
the form of URL's that refer by default to files in the user's home directory
in HDFS.  Hence, in this case, input will come from 'input-naleo-jun-21'
in HDFS under the user's home directory, and output will likewise go to
'output-naleo' in HDFS. (This directory must not exist.  To get rid of an
existing directory in HDFS, use 'hadoop fs -rmr'.)

Note that, under the hood, the actual difference is that running
'poligrounder --hadoop-nondist' will execute a command line similar to this:

$ java -classpath <SOME-LONG-CLASSPATH> opennlp.textgrounder.preprocess.ParseTweets input-naleo-jun-21 output-naleo

whereas 'poligrounder --hadoop' will execute something like this:

$ hadoop jar $POLIGROUNDER_DIR/target/PoliGrounder-assembly-0.1.0.jar opennlp.textgrounder.preprocess.ParseTweets input-naleo-jun-21 output-naleo


------ Copy data out of HDFS ------

Use 'hadoop fs -get'.


=============
Output format
=============

The normal output format from ParseTweets is called the "TextGrounder corpus"
format.  This format stores data in as a tab-separated database, with one item
per line.  There will be one or more files containing data, ending in
'-SUFFIX.txt' (or '-SUFFIX.txt.bz2' or '-SUFFIX.txt.gz', if the data is
compressed), where SUFFIX is an identifier referring to the particular sort
of data stored. (E.g. tweet data uses the suffix 'tweets'.) In addition, there
is a single file called a "schema", whose name ends in '-SUFFIX-schema.txt'.
This specifies the name of each of the fields, as well as the name and value
of any "fixed" fields (which, conceptually, have the same value for all rows,
and are used to store extra info about the corpus).  Generally, there will
be multiple output files when there are multiple reducers in Hadoop, since
each reducer produces its own output file.  Storing the data this way makes
it easy to read the files from Hadoop.  Large data files are typically
stored compressed, and are automatically uncompressed as they are read in.


=========
About SBT
=========

SBT ("Scala Build Tool", originally "Simple Build Tool") is a build tool
for building Scala-based or mixed Scala/Java-based applications.  It is
similar to 'ant' or 'maven' in that it provides the basic functionality
for recompiling only targets that need building (as in 'make'), and also
provides automatic dependency management, i.e. it will download from the
Internet, as needed, any JAR's that are needed to build the project.
The project file containing SBT directives for building a given project
is called 'build.sbt' and located in the top-level directory of the project.
The directives in build.sbt are themselves written in Scala, and
project-specific extensions are also written in Scala. (However, the
entire build.sbt is not a Scala program in its own right, because SBT
parses and executes build.sbt in a special way.) These extensions are
located in the 'project' subdirectory; this is also where SBT plugins
are specified. (For example, Poligrounder uses the 'sbt-assembly' plugin
to provide the 'assembly' task for building self-contained JAR
assemblies.)

SBT uses Apache Ivy for dependency management.

The following are some of the most useful SBT tasks, which can be specified
after the 'sbt' script or after 'poligrounder build'.

   'clean': Delete all the built files (e.g. compiled Java class files).

   'update': Download the necessary JAR's.  Note that this will locate
      the version of Scoobi built above, which is version
      '0.6.0-cdh3-SNAPSHOT-benwing'.

   'compile': Build the Java class files.  These are stored in the
      subdirectory 'target' of the project.

   'publish-local': "Publish" a built JAR file into the local Ivy repository
      stored in ~/.ivy2.

   'console': Open up the Scala interpreter with the class path set
      appropriately so that all project and dependent JAR's are available.
      The Scala interpreter can be used to execute Scala code directly,
      in order to explore Scala or test out the operation of certain
      classes and methods.  Note that if you have installed Scala yourself,
      the interpreter is what you get if you just type 'scala'; but
      normally this will not have the class path set appropriately.

   'assembly': Provided that the 'sbt-assembly' plugin is available (as in
      Poligrounder), this builds a self-contained JAR assembly containing
      the project JAR and all dependent JAR's, for use in Hadoop.

Note that more than one task can be given in a single command line, e.g.

   $ poligrounder build clean update compile


If you run 'sbt' or 'poligrounder build' with no further arguments, you
will be dropped into the SBT console.  From here, you can type in tasks,
which will be executed.  An additionally useful directive from the
console is '~ compile', which will watch for changes to source files and
then automatically recompile any time a change is seen.


===================
Running ParseTweets
===================

ParseTweets can do a lot of things.  By default, it reads in tweets in JSON
format (the raw format from Twitter) and converts them into the TextGrounder
corpus format.  However, it can also handle different formats on input
and output, as well as group or filter the tweets.

------ Grouping ------

Grouping is specified using '--grouping'.  Tweets can be grouped by user,
by timeslice, or by input file.  All tweets to be grouped together will
be compiled into a single "aggregated tweet", which will be output as if
it were a single tweet.
