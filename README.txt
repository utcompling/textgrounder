This file contains documentation on the TextGrounder application, including
an overview as well as configuration and build instructions. 

TextGrounder is open-source software, stored here:

https://github.com/utcompling/textgrounder

TextGrounder is a system for document-level and toponym-level geolocation,
aka geotagging (i.e.  identification of the geographic location -- as a pair
of latitude/longitude coordinates, somewhere on the Earth -- either of
individual toponyms in a document or of the entire document).

It also contains some political applications in the Poligrounder subproject,
for identifying relevant features in a collection of tweets that change
across the occurrence of an event (e.g. a policy announcement, presidential
debate, etc.).

There is also an out-of-date wiki, which may have more information:

https://bitbucket.org/utcompling/textgrounder/wiki/Home

This file is intended as a general introduction.  Other README files
provide more information on the various subprojects (e.g. README.geolocate,
README.poligrounder) and on preprocessing to generate corpora
(README.preprocessing).

============================
Introduction to TextGrounder
============================

TextGrounder consists of a number of applications, all of which are written
in Scala.  Scala is a hybrid object-oriented/functional language built on
top of the Java JVM.  This means that Scala code compiles to Java bytecode
and can be run just like any Java app, and that Scala and Java code can be
freely intermixed.  In many ways, Scala is a "better Java" that provides
numerous features not present in Java (although many are in C#), and makes
coding Java-type apps easier, faster and less painful.

============
Requirements
============

* Version 1.6 of the Java 2 SDK (http://java.sun.com)
* git, for downloading the code of TextGrounder and Scoobi
* A modified version of Scoobi (see below)
* Version 0.12 of SBT (used for building Scala apps, particularly Scoobi)
* Hadoop 0.20.2, particularly the Cloudera cdh3 build
* Appropriate data files, consisting of tweets (see below)

===========
Subprojects
===========

There are three subprojects:

-- The Geolocate subproject primarily does document-level geolocation,
   but also includes some code to do toponym geolocation.  This subproject
   uses a statistical model derived from a large corpus of already geolocated
   documents, such as Wikipedia or Twitter -- i.e. it is supervised (at least
   in some sense).

-- The Toponym subproject includes various algorithms for toponym-level
   geolocation.  Some of these applications build on top of the Geolocate
   subproject.

-- The Poligrounder subproject contains some political applications for
   identifying relevant features in a collection of tweets that change
   across the occurrence of an event.  This is intended for political
   applications (e.g. differences between liberals and conservatives
   across an announcement of a new policy by a president, a presidential
   debate, etc.).  However, it could potentially be applied to other
   applications.

------ Introduction to the Geolocate Subproject ------

The software implements the experiments described in the following paper:

Benjamin Wing and Jason Baldridge (2011), "Simple Supervised Document
Geolocation with Geodesic Grids." In Proceedings of the 49th Annual
Meeting of the Association for Computational Linguistics: Human Language
Technologies, Portland, Oregon, USA, June 2011.

(See http://www.jasonbaldridge.com/papers/wing-baldridge-acl2011.pdf.)

If you use this system in academic research with published results,
please cite this paper, or use this Bibtex:

{{{
@InProceedings{wing-baldridge:2011:ACL,
  author    = {Wing, Benjamin and Baldridge, Jason},
  title     = {Simple Supervised Document Geolocation with Geodesic Grids},
  booktitle = {Proceedings of the 49th Annual Meeting of the Association for Com
putational Linguistics: Human Language Technologies},
  month     = {June},
  year      = {2011},
  address   = {Portland, Oregon, USA},
  publisher = {Association for Computational Linguistics}
}
}}}

There are three main apps, each of which does a different task:

1. Document geolocation.  This identifies the location of a document.
   Training documents are currently described simply by (smoothed) unigram
   or bigram distributions, i.e. counts of the words, or combinations of
   two words, seen in the document.  In addition, each document optionally
   can be tagged with a location (specified as a pair of latitude/longitude
   values), used for training and evaluation; some other per-document data
   exists as well, much of it currently underused or not used at all.  The
   documents themselves are often Wikipedia articles, but the source data
   can come from anywhere (e.g. Twitter feeds, i.e. concatenation of all
   tweets from a given user).  Evaluation is either on documents from the
   same source as the training documents, or from some other source, e.g.
   chapters from books stored in PCL-Travel format.

2. Toponym geolocation.  This is an old, partly-written application that
   is kept around mostly for test purposes.  The real toponym-disambiguation
   applications are part of the Toponym subproject; see below.

   (Original documentation: This disambiguates each toponym in a document,
   where a toponym is a word such as "London" or "Georgia" that refers
   to a geographic entity such as a city, state, province or country.
   A statistical model is created from document data, as above, but a
   gazetteer can also be used as an additional source of data listing
   valid toponyms.  Evaluation is either on the geographic names in a
   TR-CONLL corpus or links extracted from a Wikipedia article.)

3. KML generation.  This generates per-word cell distributions of the
   sort used in the ACP strategy (--strategy=average-cell-probability),
   then outputs KML files for given words showing the distribution of
   the words across the Earth.

A fourth, not-yet-written app is for simultaneous segmentation and
geolocation.  This assumes that a document is composed of segments of
unknown size, each of which refers to a different location, and
simultaneously finds the best segmentation and best location of each
segment.

------ Introduction to the Toponym Subproject ------

WRITE ME.

------ Introduction to the Poligrounder Subproject ------

The applications related to the political subsystem (Poligrounder) are
as follows:

1. opennlp.textgrounder.preprocess.FindPolitical: This locates "ideological
   users" and identifies their ideology, based on an initial set of known
   ideological users (e.g. a set of political office-holders and their
   party identifications).  It also looks for ideologically-based features,
   e.g. hashtags, and provides an ideology for each.
2. opennlp.textgrounder.poligrounder.PoligrounderApp: Does two-way
   before/after or four-way before/after, liberal/conservative comparisons
   in a corpus, looking for words that differ across subcorpora and
   sorting by log-likelihood.


===================
Building the system
===================

The following describes how to quickly get TextGrounder up and running and to
test that everything is in place and working.

------ I. Java and Git ------

1. Make sure you have Java installed.  TextGrounder is developed and tested
   on Java 6, but it might work on Java 5 (definitely not earlier).  Scoobi
   and SBT also both depend on Java, preferably Java 6.

2. Set your JAVA_HOME environment variable to the top level of the Java
   installation tree.  On Mac OS X it's probably /Library/Java/Home.  On
   the University of Maryland (UMD) CLIP machines running Red Hat Enterprise
   Linux, it's probably /usr/lib/jvm/jre-1.6.0-sun.x86_64.

3. Make sure you have Git installed. 


II. Download SBT ------

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


------ III. Download and build Ben's modified Scoobi ------

This app uses a patched version of Scoobi (https://github.com/nicta/scoobi),
which is a toolkit for building higher-level Hadoop-based apps using Scala.
Scoobi uses Hadoop under the hood, but provides a much higher-level interface
that lets you do functional-programming-style data processing (mapping,
filtering, etc. of lists) almost exactly as if the data was stored as lists
in local memory.  The operations are automatically converted under the hood
into MapReduce steps.


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


------ IV. Download and build TextGrounder ------

1. Download TextGrounder.

   In some work directory, execute the following:

   $ git clone https://github.com/utcompling/textgrounder
   $ cd textgrounder
   $ git checkout textgrounder

2. Set up environment variables and paths.

   Set TEXTGROUNDER_DIR to the top-level directory of where TextGrounder
   is located.  Make sure $TEXTGROUNDER_DIR/bin is in your PATH, so the
   scripts in the 'bin' directory get found automatically.

3. Build TextGrounder.

   Execute the following:

   $ textgrounder build compile

   It should be possible to execute this from any directory; it uses a script
   in $TEXTGROUNDER_DIR/bin and relies on the setting in TEXTGROUNDER_DIR
   to know where the source code is.  There is a copy of SBT 0.12.0
   located inside of TextGrounder, and everything after 'textgrounder build'
   actually gets passed directly to SBT.

   In this case, the 'compile' directive causes a bunch more JAR's to get
   downloaded, and then TextGrounder gets compiled.


------ V. Get the data ------

The data necessary for running depends on the intended application.

1. For the Geolocate subproject:

   1. If you are on a UTexas Comp Ling machine or the UTexas Longhorn cluster,
      the data is already there.  Just set the environment variable
      TG_ON_COMP_LING_MACHINES to something non-blank if you're on a UTexas
      Comp Ling machine; likewise for TG_ON_LONGHORN if you're on Longhorn.
   2. If you have access to either of the above machines, look in
      `bin/config-geolocate` to see where the environment variable
      TG_GROUPS_DIR points to, and copy the relevant TextGrounder corpora out
      of the `.../corpora` directory; these will generally be the directories
      `wikipedia`, `twitter-geotext`, anything beginning with `gut...`, and
      anything beginning with `ut...`.   You might also want to copy the
      `.../projects/textgrounder` and `.../projects/pcl_travel` directories,
      especially if you're doing work with toponym resolution.  All of these
      directories should be placed under some directory with a structure
      that mirrors the source structure, starting with the `.../corpora`
      or `.../projects` directories.  Then set TG_GROUPS_DIR to the
      top-level directory into which you placed these various directories.
   3. Otherwise, you might be able to download the data and set up the
      appropriate environment variables, as described below.
   4. Otherwise, you can generate at least Wikipedia and Twitter data from
      Wikipedia dumps (publically available) and JSON-format tweets pulled
      using the Twitter API.  As described in `config-geolocate`, the
      variable TG_CORPUS_DIR points to the directory holding the TextGrounder
      corpora and can be set directly, or TG_GROUPS_DIR can be set if the
      TextGrounder corpora are held in a directory named `.../corpora`.

2. For the Toponym subproject:

WRITE ME.

3. For the Poligrounder subproject:

Poligrounder works with tweets.  Ben Wing has a very large collection of
tweets that have been downloaded over the last year or so, but only some
of the collection is currently located in UMD machines (this is because
there isn't sufficient space for everything).

The UMD data is in /fs/clip-political.  The tweets in particular are under
/fs/clip-political/corpora/twitter-pull.  Under this directory, the
'spritzer' directory contains part of the collection of tweets downloaded
using the Twitter "spritzer" mechanism.


------ VI. Test locally ------

Again, this depends somewhat on the intended application.  In general, there
are three possible running modes, some or all of which may be implemented
in a particular application:

1. Hadoop mode. Hadoop is a framework for large-scale parallel processing,
   based on Google's internal MapReduce framework. It is based on a network
   of loosely-coupled, potentially heterogeneous, potentially unreliable
   compute nodes. Network traffic is minimized by storing the data on the
   compute nodes themselves and moving the compute tasks as close as possible
   to the data, ideally on the same compute node holding the data. A certain
   amount of effort is required to set up a Hadoop cluster; most users will
   rely on an already-existing cluster.

   When running in this mode, you need to have a working Hadoop cluster
   available, and generally need to log into the Hadoop job tracker machine.
   (The machines in a cluster are divided into job trackers, name nodes,
   and task trackers.  Most commonly, there is one "master" machine that acts
   simultaneously as job tracker and name node, while all the other "slave"
   machines serve as task trackers and do the actual computing.)

2. Hadoop non-distributed mode.  This uses the Hadoop application framework
   but does not require that any actual Hadoop cluster exist or any
   Hadoop-related setup to be done at all. (The necessary Hadoop libraries
   were automatically downloaded during the build process, and the cluster is
   simulated on the local machine by these libraries.) This is useful for
   testing, or if no Hadoop cluster is available.  If an application
   supports Hadoop, it can be run in either regular Hadoop mode or Hadoop
   non-distributed mode.

3. Standalone mode.  Standalone applications do not use Hadoop at all, and
   simply run on the local machine.  Some standalone applications are able
   to do a certain amount of parallel processing by automatically making use
   of multiple cores in the local machine, if they exist (which they do in
   most modern desktops and laptops).  This feature is based on the
   parallel-collections facility built into Scala.


1. For the Geolocate subproject:

The applications in the Geolocate project can be run either as Hadoop
applications or as standalone applications, and even in standalone alone
will make use of multiple cores if they are available.  See above for more
details.

A good test corpus is the GeoText Twitter corpus, which is very small.
Running time on this corpus should be on the order of a minute or two.

To test in standalone mode, try the following:

$ tg-geolocate geotext

If you have appropriately set up all your data files and set the environment
variable(s) needed to tell TextGrounder where they are located, this should
run on the corpus.  By default, this will produce a lot of output, documenting
the operation on each test file.  At the end, there will be some output
describing the overall results.  The current run reports a mean distance error
of about 941 kilometers, and a median distance error of about 505 kilometers.
(NOTE: This may change!  In particular the results may get better.  But if your
numbers are significantly larger, or enormously smaller, something is wrong.)

Note that `tg-geolocate` is a front-end script onto the more general script
`textgrounder`, which in turn is a front-end script for running applications
either in standalone mode, in Hadoop non-distributed mode, or in Hadoop
cluster mode.  In the former two cases, the applications end up being run using
`java`, while in the latter case using the `hadoop` wrapper. `textgrounder`
sets up the Java class path properly and invokes either `java` or `hadoop`
as required, while `tg-geolocate` calls `textgrounder` on the app class
`opennlp.textgrounder.geolocate.Geolocate`, specifying some relevant options
and in particular determining the proper path for the relevant corpus (e.g.
`geotext`).

If something goes wrong, or you just want to see more exactly what is being
run, use the `--verbose` option to `tg-geolocate` or `textgrounder`, e.g.:

$ tg-geolocate --verbose geotext

This will output the exact command lines being executed.

Once this works, try testing in Hadoop non-distributed mode:

$ tg-geolocate --hadoop-nondist geotext output

The argument `output` specifies the name of a directory to store the output
results in -- in this case, a subdirectory of the current directory named
`output`.  This must not exist already.  Not very much output will be printed
to stdout, but after the program finishes, there should be a subdirectory
called `output`, with the results stored in a file called `part-r-00000` or
similar.

2. For the Toponym subproject:

WRITE ME.

3. For the Poligrounder subproject:

As described in an earlier section, there are three applications in this
subproject, and as described above, some can use Hadoop while others are
standalone.  The application known a PoligrounderApp uses the same application
framework as the applications in the Geolocate subproject (see above), although
currently it runs only in standalone mode (with support for multiple cores).
The other applications are based on Scoobi, a Scala interface onto Hadoop,
and consequently can only be run in one of the Hadoop modes.

The first thing to do is to test the ParseTweets app in Hadoop non-distributed
mode.

A simple test corpus is the 'input-naleo-jun-21' corpus, containing about
3,300 tweets.  On the UMD machines, this is located under
/fs/clip-political/corpora.  Hence, on these machines you should be able to run
the following:

$ textgrounder --verbose --hadoop-nondist run opennlp.textgrounder.preprocess.ParseTweets /fs/clip-political/corpora/input-naleo-jun-21 output-naleo

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


------ VII. Test under Hadoop ------

To run under Hadoop (normally using a cluster, i.e. not in non-distributed
mode), you will need to do the following steps, in general:

-- Make sure you have a self-contained JAR assembly built.
-- Request machines, for running Hadoop, if necessary.
-- Log into the Hadoop job tracker.
-- Copy your data into HDFS (the Hadoop Distributed File System), unless
   your data is stored on a local file system that is visible to *all*
   Hadoop nodes (the job tracker and all task trackers).
-- Submit the job to Hadoop.
-- Copy your data out of HDFS.

This applies to all subprojects.

Running Hadoop on a cluster is identical to running locally (in
non-distributed mode), except for the following differences:

1. Use the `--hadoop` option to `textgrounder` instead of `--hadoop-nondist`.
   Under the hood, this means that the main and all dependent JAR files are
   bundled into a single JAR assembly, and the application class is run
   using the `hadoop` wrapper rather than directly using `java`. (See the
   section below on running under Hadoop.)

2. The input and output directories/specs normally refer to HDFS rather than
   the local file system.  On input only, a local path can specifically be
   given using a URL-style spec beginning with file:///.

You can see the output as follows:

$ hadoop fs -ls

If you named your output directory `output, you should see `output` as one
of the directories.  If this directory already exists, you will get an error;
choose a different name, or use `hadoop fs -rmr output` to remove it first.
The results are stored in multiple files.  You can see the total output using
`hadoop fs -cat`, e.g.

$ hadoop fs -cat 'output/*'


===============================
More about running under Hadoop
===============================


1. Building an assembly

To run TextGrounder under Hadoop, it's necessary to build a self-contained
"assembly" or "fat JAR" containing the TextGrounder code as well as all the
dependent JAR's necessary to run TextGrounder.

To do this, execute the following:

$ textgrounder build assembly

Note that this step may take several minutes on some machines.


2. Running Hadoop apps

In general, Hadoop apps are run as follows:

$ textgrounder --hadoop [OTHER-TEXTGROUNDER-WRAPPER-OPTIONS] run <CLASS-OF-APP-TO-RUN> [HADOOP-APP-OPTIONS] [APP-SPECIFIC-OPTIONS] <INPUT-SPEC> <OUTPUT-SPEC>

The [HADOOP-APP-OPTIONS] must come before any app-specific options, and are
options related to the Hadoop application framework on which the app is built.
Examples are specifying the values of various Hadoop-related configuration
variables.

<INPUT-SPEC> specifies either a file to process or a directory containing files
to be processed.  Glob specs are common, allowing multiple files/directories
to be specified.  Whichever set of files ends up being indicated, the data in
these files is effectively concatenated to form the input.  Most commonly, a
single directory is specified.

<OUTPUT-SPEC> specifies a directory to store output results in.  There will
in general be one output file per Hadoop reducer task.

As mentioned above, both input and output directories are in the form of URL's
that refer by default to files in the user's home directory in HDFS.
(This directory must not exist.  To get rid of an existing directory in HDFS,
use 'hadoop fs -rmr'.)


3. Under the hood

When running using the `textgrounder` front end, the difference between
cluster and non-distributed modes is simply the difference between the
`--hadoop` and `--hadoop-nondist` options, e.g. for cluster mode:

$ textgrounder --verbose --hadoop run opennlp.textgrounder.preprocess.ParseTweets input-naleo-jun-21 output-naleo

Note that, under the hood, the actual difference is that running
`textgrounder --hadoop-nondist` will execute a command line similar to this:

$ java -classpath <SOME-LONG-CLASSPATH> opennlp.textgrounder.preprocess.ParseTweets input-naleo-jun-21 output-naleo

whereas `textgrounder --hadoop` will execute something like this:

$ hadoop jar $TEXTGROUNDER_DIR/target/PoliGrounder-assembly-0.1.0.jar opennlp.textgrounder.preprocess.ParseTweets input-naleo-jun-21 output-naleo


In general, cluster mode `textgrounder` ends up calling something like the
following:

$ hadoop [HADOOP-WRAPPER-OPTIONS] jar <JAR-ASSEMBLY-FILE> <CLASS-OF-APP-TO-RUN> [HADOOP-APP-OPTIONS] [APP-SPECIFIC-OPTIONS] <INPUT-SPEC> <OUTPUT-SPEC>

Note that everything starting with the class of the application to run is the
same in both methods.


4. Example

$ textgrounder --hadoop opennlp.textgrounder.preprocess.ParseTweets --grouping file input output

This boils down to:

$ hadoop jar $TEXTGROUNDER_DIR/target/textgrounder-assembly.jar opennlp.textgrounder.preprocess.ParseTweets --grouping file input output

This assumes that the data to be preprocessed is located in the HDFS directory
`input` and the results are stored into the HDFS directory `output`.  All the
files in the `input` directory will be logically concatenated and then
processed; in this case, `ParseTweets` will take what are assumed to be
JSON-format tweets, group them by user and output results in the textdb format
(see `README.preprocess`).


5. Local vs. HDFS input

Relative or simple absolute file names given a Hadoop application refer to
Hadoop's own HDFS (Hadoop Distributed File System), not to the normal file
system on a given machine (unless running in non-distributed mode, in which
case all files are stored on the local file system).  HDFS is implemented by
the Hadoop servers running on a cluster and stores data under particular
subdirectories (e.g. `/hadoop`) spread out across the various task trackers
in a cluster.  HDFS is most efficient when storing large files (around 2x or
more the block size; i.e. at least about 128 MB for a typical 64 MB block
size).  HDFS is optimized for throughput over latency, i.e. it will process
large amounts of data quickly but may have a significant start-up overhead.
This trade-off is the reason why such large block sizes are used.

It is also possible to have a Hadoop app read directly from the local file
system, using a URL-style file name beginning with file:///.  This however
requires that the same files be visible on all machines in the Hadoop cluster
(i.e. on the job tracker and all task trackers).

Theoretically, reading a large amount of data from HDFS is faster and more
efficient than an equivalent amount of data stored in a distributed local file
system (e.g. NFS or Lustre), because when data is stored in HDFS, Hadoop is
able to take advantage of data locality by moving the tasks to the machines
where the data is stored locally.

However, if the data is stored in compressed format (see below), the difference
becomes less significant because of the amount of time required to uncompress
the data, which is the same regardless of where the data is stored and may
dwarf the data read time (depending on the speed of the network, the speed of
the file system, the speed of the processor, the load on the processor, the
complexity of the decompression algorithm, etc.).

Furthermore, this gain from using HDFS is only possible at all if the data is
already stored in HDFS.  If the data is only available locally and first needs
to be copied into HDFS, there is no point whatsoever in doing this unless the
same data will be processed multiple times under Hadoop and can be left in HDFS
for the duration of the entire running process.  In general, this is not
possible on the TACC cluster; hence, it is better to simply access the data
locally.

Output is always written to HDFS, and must be copied out if it is needed on
a local file system.

As a matter of safety, HDFS is not overly reliable, and should not be counted
on as the primary storage mechanism for long-term storage. (Furthermore, on
TACC, each HDFS partition needs to be created anew for each compute-server
request, and will subsequently disappear when the request ends, i.e. in at
most 24 hours  Hence, it is critical to copy output data out of HDFS *before*
the request is terminated.)


6. Compressed input

Files that are compressed using GZIP or BZIP2 will automatically be
decompressed.  Note that decompression will take some time, especially of
BZIP2, and may in fact be the dominating factor in the processing time.
Furthermore, GZIP files can't be split by Hadoop, and BZIP2 files can't be
split unless you're using Hadoop 0.21 or later, and Hadoop will choke on
combined BZIP2 files (created by concatenating multiple individual BZIP2 files)
unless you're using Hadoop 0.21 or later.  Hence unless splitting is possible,
you should limit the size of each input file to about 1 GB uncompressed, or
about 100 MB compressed; that's maybe 24 hours worth of downloading tweets
from Twitter's Spritzer mechanism as of June 2012.


7. Running under Bespin

The Bespin cluster at the University of Maryland is a Hadoop cluster of 16
machines, each with 8 cores.  The name node is 'bespinsub00.umiacs.umd.edu'.
It's not necessary to request any compute servers beforehand, but data must
be copied into HDFS, since the /fs/clip-political file system is not visible
to the task nodes.


8. Running under TACC Longhorn

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


9. Copy data into HDFS

If it is necessary to copy data into HDFS, use 'hadoop fs -put' to copy
a directory tree into HDFS, which can then be accessed using a relative
URL.  If data is to be read from a local file system, it will need to be
accessed using an absolute URL with the 'file:///' prefix, e.g. on Longhorn:

file:///scratch/01683/benwing/corpora/twitter-pull/all-spritzer


10. Copy data out of HDFS

Use 'hadoop fs -get'.


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
are specified. (For example, TextGrounder uses the 'sbt-assembly' plugin
to provide the 'assembly' task for building self-contained JAR
assemblies.)

SBT uses Apache Ivy for dependency management.

The following are some of the most useful SBT tasks, which can be specified
after the 'sbt' script or after 'textgrounder build'.

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
      TextGrounder), this builds a self-contained JAR assembly containing
      the project JAR and all dependent JAR's, for use in Hadoop.

Note that more than one task can be given in a single command line, e.g.

   $ textgrounder build clean update compile


If you run 'sbt' or 'textgrounder build' with no further arguments, you
will be dropped into the SBT console.  From here, you can type in tasks,
which will be executed.  An additionally useful directive from the
console is '~ compile', which will watch for changes to source files and
then automatically recompile any time a change is seen.


===========
Bug Reports
===========

Please report bugs by sending mail to jbaldrid at mail.utexas.edu.

