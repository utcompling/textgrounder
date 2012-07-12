This file contains documentation on the TextGrounder project, including
an overview as well as configuration and build instructions. 

See CHANGES for a description of the project status. 

See also the TextGrounder website:

https://bitbucket.org/utcompling/textgrounder/

In particular, see the wiki, which may have more detailed and/or up-to-date
information:

https://bitbucket.org/utcompling/textgrounder/wiki/Home

============
Introduction
============

TextGrounder does document-level and toponym-level geolocation, aka
geotagging (i.e.  identification of the geographic location -- as a pair
of latitude/longitude coordinates, somewhere on the Earth -- either of
individual toponyms in a document or of the entire document).

There are two subprojects.  The Geolocate subproject primarily does
document-level geolocation, but also includes some code to do toponym
geolocation.  The Toponym subproject includes various algorithms for
toponym-level geolocation.  The Geolocate subproject uses a statistical
model derived from a large corpus of already geolocated documents,
such as Wikipedia -- i.e. it is supervised (at least in some sense).
The Toponym subproject primarily relies only on a Gazetteer -- i.e.
a list of possible toponyms; hence it is more unsupervised.

========================================
Introduction to the Geolocate Subproject
========================================

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

2. Toponym geolocation.  This disambiguates each toponym in a document,
   where a toponym is a word such as "London" or "Georgia" that refers
   to a geographic entity such as a city, state, province or country.
   A statistical model is created from document data, as above, but a
   gazetteer can also be used as an additional source of data listing
   valid toponyms.  Evaluation is either on the geographic names in a
   TR-CONLL corpus or links extracted from a Wikipedia article.

3. KML generation.  This generates per-word cell distributions of the
   sort used in the ACP strategy (--strategy=average-cell-probability),
   then outputs KML files for given words showing the distribution of
   the words across the Earth.

A fourth, not-yet-written app is for simultaneous segmentation and
geolocation.  This assumes that a document is composed of segments of
unknown size, each of which refers to a different location, and
simultaneously finds the best segmentation and best location of each
segment.

============
Requirements
============

* Version 1.6 of the Java 2 SDK (http://java.sun.com)
* If you want to run using Hadoop, version 0.20.2 of Hadoop
* Appropriate data files (see below)


===========
Quick start
===========

The following describes how to quickly get the Geolocate subproject
up and running and to test that everything is in place and working.

1. Download TextGrounder.  Probably the easiest way is through Mercurial
   (http://mercurial.selenic.com), an application for source-code management.
   If you don't have Mercurial, you can download it from the Mercurial web
   site, or (on Mac OS X) install it using MacPorts (http://www.macports.org);
   you might need to install the Mac OS X Developer Tools, aka Xcode
   (http://developer.apple.com/mac/), in order to install MacPorts.
   Mercurial should also be available as a package in all the standard Linux
   distributions.  Once you have Mercurial, get TextGrounder like this:

   hg clone https://bitbucket.org/utcompling/textgrounder/

2. Make sure you have Java installed.  TextGrounder is developed and tested
   on Java 6, but it might work on Java 5 (definitely not earlier).

3. Set up environment variables and paths:

   -- Set TEXTGROUNDER_DIR to the top-level directory of where TextGrounder
      is located.
   -- Also make sure JAVA_HOME is set to the top level of the Java
      installation tree. (On Mac OS X it's probably /Library/Java/Home.)
   -- Make sure $TEXTGROUNDER_DIR/bin is in your PATH, so the scripts in
      the 'bin' directory get found automatically.
   -- See below if you're not sure how to set environment variables.

4. Obtain the data.

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

5. Build the system.

   Run the following:

   textgrounder build-all

   This should download a whole bunch of stuff and then compile the whole
   system.

6. Test the system.

   Run the following:

   tg-geolocate geotext

   If all is well, this should do document-level geolocation on the GeoText
   Twitter corpus with a grid size of 1 degree.  Because this is a small
   corpus, this should take only 30 seconds to a minute.  The final answers
   should have a mean of about 632 miles and a median of about 402 miles.

======================================
Configuring your environment variables
======================================

The easiest thing to do is to set the environment variables JAVA_HOME
and TEXTGROUNDER_DIR to the relevant locations on your system. Set JAVA_HOME
to match the top level directory containing the Java installation you
want to use.

For example, on Windows:

C:\> set JAVA_HOME=C:\Program Files\jdk1.5.0_04

or on Unix:

% setenv JAVA_HOME /usr/local/java
  (csh)
> export JAVA_HOME=/usr/java
  (ksh, bash)

On Windows, to get these settings to persist, it's actually easiest to
set your environment variables through the System Properties from the
Control Panel. For example, under WinXP, go to Control Panel, click on
System Properties, choose the Advanced tab, click on Environment
Variables, and add your settings in the User variables area.

/*Next, likewise set TEXTGROUNDER_DIR to be the top level directory where you
unzipped the download. In Unix, type 'pwd' in the directory where
this file is and use the path given to you by the shell as
TEXTGROUNDER_DIR.  You can set this in the same manner as for JAVA_HOME
above.*/ //THIS WILL BE REMOVED

Next, add the directory TEXTGROUNDER_DIR/bin to your path. For example, you
can set the path in your .bashrc file as follows:

export PATH="$PATH:$TEXTGROUNDER_DIR/bin"

On Windows, you should also add the python main directory to your path.

Once you have taken care of these three things, you should be able to
build and use the TextGrounder Library.

Note: Spaces are allowed in JAVA_HOME but not in TEXTGROUNDER_DIR.  To set
an environment variable with spaces in it, you need to put quotes around
the value when on Unix, but you must *NOT* do this when under Windows.


===============================
Building the system from source
===============================

TextGrounder uses SBT (Simple Build Tool) with a standard directory
structure.  To build TextGrounder, type (this works in any directory,
provided you have set TEXTGROUNDER_DIR correctly):

$ textgrounder build update compile

This will compile the source files and put them in
./target/classes. If this is your first time running it, you will see
messages about Scala being dowloaded -- this is fine and
expected. Once that is over, the TextGrounder code will be compiled.

To try out other build targets, do:

$ textgrounder build

This will drop you into the SBT interface. To see a list of possible
actions, type 'tasks'.  (You an also see all the actions that are
possible by hitting the TAB key.  In general, you can do auto-completion
on any command prefix in SBT, hurrah!)

A particularly useful action during development is '~ compile'.  This
does "continuous compilation", i.e. it automatically recompiles every
time a change is made to a source file. (This may produce lots of errors
in the middle of a development cycle, but this process is harmless,
and eventually you shouldn't get any more errors.)  Also try 'clean'
followed by 'compile' if you get some really weird compile errors that
you don't think should be there.

Documentation for SBT is here:

https://github.com/harrah/xsbt/wiki

Note: if you have SBT 0.11.3 already installed on your system, you can
also just call it directly with "sbt" in TEXTGROUNDER_DIR.


==============================
Building and running on Hadoop
==============================

The first thing is to do a test in non-distributed mode.  This should
require no additional steps from running with Hadoop, and doesn't even
require that you have an installation of Hadoop, since we automatically
download the Hadoop libraries during the build step (specifically, when
the 'update' task of SBT is used).  As an example, to run the Geolocate
subproject on the GeoText Twitter corpus (which is very small and hence
is a good test):

$ tg-geolocate --hadoop-nondist geotext output

If you have appropriately set up all your data files and set the environment
variable(s) needed to tell TextGrounder where they are located, this should
run on the corpus.  The results should appear inside of a subdirectory called
`output`, in a file called `part-r-00000` or similar.  The current run reports
a mean distance error of about 941 kilometers, and a median distance error of
about 505 kilometers. (NOTE: This may change!  In particular the results may
get better.  But if your numbers are significantly larger, or enormously
smaller, something is wrong.)

To run properly using Hadoop (in distributed mode), you need an additional
build step:

$ textgrounder build assembly

This builds a self-contained JAR file containing not only the TextGrounder
code but all of the libraries needed to run the code.

There are two ways of accessing the data files, either through the local
file system or through HDFS (the Hadoop Distributed File System).  The latter
way may be more efficient in that Hadoop is optimized for HDFS, but it
requires a bit more work, in that the data needs to be copied into HDFS.
The former does not require this, but does require that the relevant files
are visible on all nodes that will be running the program.

-------- Running Hadoop normally (distributed mode), with local data ----------

To run Hadoop normally (in distributed mode, using multiple compute nodes
as necessary, rather than simulating it using non-distributed mode, as
described above), but with data accessed through the local file system,
simply add the '--hadoop' option, for example:

$ tg-geolocate --hadoop geotext output

This will invoke 'hadoop' to run the code.  Again, `output` is the
subdirectory to store the results in; in this case, it is located on HDFS.
You can see this as follows:

$ hadoop fs -ls

You should see `output` as one of the directories.  If this directory already
exists, you will get an error; choose a different name, or use
`hadoop fs -rmr output` to remove it first.  The results should appear inside
of `output`, in a file called `part-r-00000` or similar, with numbers the
same as above.  You can see the output as follows:

$ hadoop fs -cat 'output/*'

If something goes wrong, or you just want to see more exactly what is being
run, use the '--verbose' option, e.g.:

$ tg-geolocate --verbose --hadoop geotext

This will output the exact command lines being executed.

-------- Running Hadoop normally, with data in HDFS ----------

The first step is to copy the data to the Hadoop File System, which you
can do using 'tg-copy-data-to-hadoop'.  You need to copy two things,
the corpus or corpora you want to run on and some ancillary data needed
for TextGrounder (basically the stop lists).  You run the command as follows:

$ tg-copy-data-to-hadoop CORPUS ...

where CORPUS is the name of a corpus, similar to what is specified when
running 'tg-geolocate'.  Additionally, the pseudo-corpus 'textgrounder'
will copy the ancillary TextGrounder data.  For example, to copy
the Portuguese Wikipedia for March 15, 2012, as well as the ancillary data,
run the following:

$ tg-copy-data-to-hadoop textgrounder ptwiki-20120315

Other possibilities for CORPUS are 'geotext' (the Twitter GeoText corpus),
any other corpus listed in the corpus directory (TG_CORPUS_DIR), any
tree containing corpora (all corpora underneath will be copied and the
directory structure preserved), or any absolute path to a corpus or
tree of corpora.

Then, run as follows:

$ TG_USE_HDFS=yes tg-geolocate --hadoop geotext output

If you use '--verbose' as follows, you can see exactly which options are
being passed to the underlying 'textgrounder' script:

$ TG_USE_HDFS=yes tg-geolocate --hadoop --verbose geotext output

By default, the data copied using 'tg-copy-data-to-hadoop' and referenced
by 'tg-geolocate' or 'textgrounder' is placed in the directory
'textgrounder-data' under your home directory on HDFS.  You can change this
by setting TG_HADOOP_DIR before running 'tg-copy-data-to-hadoop'.

========================================
Specifying where the corpora are located
========================================

The corpora are located using the environment variable TG_CORPUS_DIR.
If this is unset, and TG_GROUPS_DIR is set, it will be initialized to
the 'corpora' subdirectory of this variable.  If you are running
directly on Longhorn, you can set TG_ON_LONGHORN to 'yes', which will
set things up appropriately to use the corpora located in
/scratch/01683/benwing/corpora.

============================================
Further Details for the Geolocate Subproject
============================================

=== Obtaining the Data ===

If you don't have the data already (you do if you have access to the Comp
Ling machines), download and unzip the processed Wikipedia/Twitter data and
aux files from http://wing.best.vwh.net/wikigrounder.

There are three sets of data to download:
  * The processed Wikipedia data, in `wikipedia/`.  The files are
    listed separately here and bzipped, in case you don't want them all.
    If you're not sure, get them all; or read down below to see which ones
    you need.
  * The processed Twitter data, in `wikigrounder-twitter-1.0.tar.bz2`.
  * Auxiliary files, in `wikigrounder-aux-1.0.tar.bz2`. NOTE: These really
    aren't needed any more.  The only remaining auxiliary file is the
    World Gazetteer, and that is needed only when doing toponym resolution.

Untar these files somewhere.  It is generally recommended that you create
a directory and set `TG_GROUPS_DIR` to point to it; then put the Wikipedia
and Twitter data underneath the `$TG_GROUPS_DIR/corpora` subdirectory, and the
auxiliary files (if needed) under `$TG_GROUPS_DIR/projects/textgrounder/data`.
Alternatively, `TG_CORPUS_DIR` can be used to directly point to where the
corpora are stored, and `TG_AUX_DIR` to directly point to where the auxiliary
files (if needed) are stored.

(Alternatively, if you are running on a UTexas Comp Ling machine, or a
machine with a copy of the relevant portions of /groups/corpora and
/groups/projects in the same places, set `TG_ON_COMP_LING_MACHINES` to a
non-empty value and it will initialize those three for you.  If you are
running on the UTexas Longhorn cluster, set `TG_ON_LONGHORN` to a non-empty
value and the variables will be initialized appropriately for Longhorn.)

The Wikipedia data was generated from [http://download.wikimedia.org/enwiki/20100904/enwiki-20100904-pages-articles.xml.bz2 the original English-language Wikipedia dump of September 4, 2010].

The Twitter data was generated from [http://www.ark.cs.cmu.edu/GeoText/ The Geo-tagged Microblog corpus] created by [http://aclweb.org/anthology-new/D/D10/D10-1124.pdf Eisenstein et al (2010)].

=== Replicating the experiments ===

The code in Geolocate.scala does the actual geolocating.  Although these
are written in Java and can conceivably be run directly using `java`,
in practice it's much more convenient using either the `textgrounder`
driver script or some other even higher-level front-end script.
`textgrounder` sets up the paths correctly so that all libraries, etc.
will be found, and takes an application to run, knowing how to map that
application to the actual class that implements the application.  Each
application typically takes various command-line arguments, and
`textgrounder` itself also takes various command-line options (given
*before* the application name), which mostly control operation of the
JVM.

In this case, document geotagging can be invoked directly with `textgrounder`
using `textgrounder geolocate-document`, but the normal route is to
go through a front-end script.  The following is a list of the front-end
scripts available:

  * `tg-geolocate` is the script you probably want to use.  It takes a
    CORPUS parameter to specify which corpus you want to act on (currently
    recognized: `geotext`; a Wikipedia corpus, e.g. `enwiki-20120307` or
    `ptwiki-20120315`; `wikipedia`, which picks some "default" Wikipedia
    corpus, specifically `enwiki-20100905` (the same one used for the
    original Wing+Baldridge paper, and quite old by now); and
    `geotext-wiki`, which is a combination of both the `wikipedia` and
    `geotext` corpora).  This sets up additional arguments to
    specify the data files for the corpus/corpora to be loaded/evaluated,
    and the language of the data, e.g. to select the correct stopwords list.
    The application to run is specified by the `--app` option; if omitted,
    it defaults to `geolocate-document` (other possibilities are
    `generate-kml` and `geolocate-toponym`).  For the Twitter corpora,
    an additional option `--doc-thresh NUM` can be used to specify the
    threshold, i.e. minimum number of documents that a vocabulary item
    must be seen in; uncommon vocabulary before that is ignored (or
    rather, converted to an OOV token).  Additional arguments to both
    the app and `textgrounder` itself can be given.  Configuration values
    (e.g. indicating where to find Wikipedia and Twitter, given the above
    environment variables) are read from `config-geolocate` in the
    TextGrounder `bin` directory; additional site-specific configuration
    will be read from `local-config-geolocate`, if you create that file
    in the `bin` directory.  There's a `sample.local-config-geolocate`
    file in the directory giving a sample local config file.

  * `tg-generate-kml` is exactly the same as `tg-geolocate --app generate-kml`
    but easier to type.

  * `geolocate-toponym` is almost exactly the same as
    `tg-geolocate --app geolocate-toponym`, but also specifies a gazetteer
    file as an extra argument.  You still need to supply a value for
    `--eval-file` and `--eval-type`.

  * `geolocate-toponym-tr-conll` is almost exactly the same as
    `geolocate-toponym`, but also specifies arguments to evaluate on the
    PCL-CoNLL corpus.

  * `run-nohup` is a script for wrapping other scripts.  The other script
    is run using `nohup`, so that a long-running experiment will not get
    terminated if your shell session ends.  In addition, starting times
    and arguments, along with all output, are logged to a file with a
    unique, not-currently existing name, where the name incorporates the
    name of the underlying script run, the current time and date, an
    optional ID string (specified using the `-i` or `--id` argument),
    and possibly an additional number needed to ensure that the file is
    unique -- it will refuse to overwrite an existing file.  This ID is
    useful for identifying different experiments using the same script.
    The experiment runner `run-geolocate-exper.py`, which allows iterating
    over different parameter settings, generates an ID based on the
    current parameter settings.

  * `python/run-geolocate-exper.py` is a framework for running a series of
    experiments on similar arguments.  It was used extensively in running
    the experiments for the paper.

You can invoke `tg-geolocate wikipedia` with no options, and it will do
something reasonable: It will attempt to geolocate the entire dev set of
the old English Wikipedia corpus, using KL divergence as a strategy, with
a grid size of 1 degrees.  Options you may find useful (which also apply to
`textgrounder geolocate` and all front ends):

`--degrees-per-cell NUM`
`--dpc NUM`

Set the size of a cell in degrees, which can be a fractional value.

`--eval-set SET`

Set the split to evaluate on, either "dev" or "test".

`--strategy STRAT ...`

Set the strategy to use for geolocating.  Sample strategies are
`partial-kl-divergence` ("KL Divergence" in the paper),
`average-cell-probability` ("ACP" in the paper),
`naive-bayes-with-baseline` ("Naive Bayes" in the paper), and `baseline`
(any of the baselines).  You can specify multiple `--strategy` options
on the command line, and the specified strategies will be tried one after
the other.

`--baseline-strategy STRAT ...`

Set the baseline strategy to use for geolocating. (It's a separate
argument because some strategies use a baseline strategy as a fallback,
and in those cases, both the strategy and baseline strategy need to be
given.) Sample strategies are `link-most-common-toponym` ("??" in the
paper), `num-documents`  ("??" in the paper), and `random` ("Random" in
the paper).  You can specify multiple `--baseline-strategy` options,
just like for `--strategy`.

`--num-training-docs, --num-test-docs`

One way of controlling how much work is done.  These specify the maximum
number of documents (training and testing, respectively) to load/evaluate.

`--max-time-per-stage SECS`
`--mts SECS`

Another way of controlling how much work is done.  Set the maximum amount
of time to spend in each "stage" of processing.  A value of 300 will
load enough to give you fairly reasonable results but not take too much
time running.

`--skip-initial N, --every-nth N`

A final way of controlling how much work is done.  `--skip-initial`
specifies a number of test documents to skip at the beginning before
stating to evaluate.  `--every-nth` processes only every Nth document
rather than all, if N > 1.  Used judiciously, they can be used to split
up a long run.

An additional argument specific to the Twitter front ends is
`--doc-thresh`, which specifies the threshold (in number of documents)
below which vocabulary is ignored.  See the paper for more details.

=== Extracting results ===

A few scripts are provided to extract the results (i.e. mean and median
errors) from a series of runs with different parameters, and output the
results either directly or sorted by error distance:

  * `extract-raw-results.sh` extracts results from a number of runs of
    any of the above front end programs.  It extracts the mean and median
    errors from each specified file, computes the avg mean/median error,
    and outputs a line giving the errors along with relevant parameters
    for that particular run.

  * `extract-results.sh` is similar but also sorts by distance (both
    median and mean, as well as avg mean/median), to see which parameter
    combinations gave the best results.

=== Specifying data ===

Data is specified using the `--input-corpus` argument, which takes a
directory.  The corpus generally contains one or more "views" on the raw
data comprising the corpus, with different views corresponding to differing
ways of representing the original text of the documents -- as raw,
word-split text (i.e. a list, in order, of the "tokens" that occur in the
text, where punctuation marks count as their own tokens and hyphenated words
may be split into separate tokens); as unigram word counts (for each unique
word -- or more properly, token -- the number of times that word occurred);
as bigram word counts (similar to unigram counts but pairs of adjacent tokens
rather than single tokens are counted); etc.  Each such view has a schema
file and one or more document files.  The schema file is a short file
describing the structure of the document files.  The document files
contain all the data for describing each document, including title, split
(training, dev or test) and other metadata, as well as the text or word
counts that are used to create the textual distribution of the document.

The document files are laid out in a very simple database format,
consisting of one document per line, where each line is composed of a
fixed number of fields, separated by TAB characters. (E.g. one field
would list the title, another the split, another all the word counts,
etc.) A separate schema file lists the name of each expected field.  Some
of these names (e.g. "title", "split", "text", "coord") have pre-defined
meanings, but arbitrary names are allowed, so that additional
corpus-specific information can be provided (e.g. retweet info for tweets
that were retweeted from some other tweet, redirect info when a Wikipedia
article is a redirect to another article, etc.).

Additional data files (which are automatically handled by the
`tg-geolocate` script) are specified using `--stopwords-file` and
`--gazetteer-file`.  The stopwords file is a list of stopwords (one per
line), i.e. words to be ignored when generating a distribution from the
word counts in the counts file.  You don't normally have to specify this
at all; if not, a default stopwords file is retrieved from inside the
TextGrounder distribution.  The optional gazetteer file is used only
when doing toponym resolution (--mode=geotag-toponyms), and doesn't
apply at all when doing the normal document resolution, as was done in
the paper.

The following is a list of the generally-applicable defined fields:

  * `title`: Title of the document.  Must be unique within a given corpus,
  and must be present.  If no title exists (e.g. for a unique tweet), but
  an ID exists, use the ID.  If neither exists, make up a unique number
  or unique identifying string of some sort.

  * `id`: The (usually) numeric ID of the document, if such a thing exists.
  Currently used only when printing out documents.  For Wikipedia articles,
  this corresponds to the internally-assigned ID.

  * `split`: One of the strings "training", "dev", "test".  Must be present.

  * `corpus`: Name of the corpus (e.g. "enwiki-20111007" for the English
  Wikipedia of October 7, 2011.  Must be present.  The combination of
  title and corpus uniquely identifies a document in the presence of
  documents from multiple corpora.

  * `coord`: Coordinates of a document, or blank.  If specified, the
  format is two floats separated by a comma, giving latitude and longitude,
  respectively (positive for north and east, negative for south and
  west).

The following is a list of fields specific to Wikipedia:

  * `redir`: If this document is a Wikipedia redirect article, this
  specifies the title of the document redirected to; otherwise, blank.
  This field is not much used by the document-geotagging code (it is
  more important during toponym geotagging).  Its main use in document
  geotagging is in computing the incoming link count of a document (see
  below).

  * `incoming_links`: Number of incoming links, or blank if unknown.
  This specifies the number of links pointing to the document from anywhere
  else.  This is primarily used as part of certain baselines (`internal-link`
  and `link-most-common-toponym`).  Note that the actual incoming link count
  of a Wikipedia article includes the incoming link counts of any redirects
  to that article.

  * `namespace`: For Wikipedia articles, the namespace of the article.
  Articles not in the `Main` namespace have the namespace attached to
  the beginning of the article name, followed by a colon (but not all
  articles with a colon in them have a namespace prefix).  The main
  significance of this field is that articles not in the `Main` namespace
  are ignored.  For documents not from Wikipedia, this field should be
  blank.

  * `is_list_of`, `is_disambig`, `is_list`: These fields should either
  have  the value of "yes" or "no".  These are Wikipedia-specific fields
  (identifying, respectively, whether the article title is "List of ...";
  whether the article is a Wikipedia "disambiguation" page; and whether
  the article is a list of any type, which includes the previous two
  categories as well as some others).  None of these fields are currently
  used.

=== Generating KML files ===

It is possible to generate KML files showing the distribution of particular
words over the Earth's surface, using `tg-generate-kml` (e.g.
`tg-generate-kml wikipedia --kml-words mountain,beach,war`).  The resulting
KML files can be viewed using [http://earth.google.com Google Earth].
The only necessary arg is `--kml-words`, a comma-separated list
of the words to generate distributions for.  Each word is saved in a
file named by appending the word to whatever is specified using
`--kml-prefix`.  Another argument is `--kml-transform`, which is used
to specify a function to apply to transform the probabilities in order
to make the distinctions among them more visible.  It can be one of
`none`, `log` and `logsquared` (actually computes the negative of the
squared log).  The argument `--kml-max-height` can be used to specify
the heights of the bars in the graph.  It is also possible to specify
the colors of the bars in the graph by modifying constants given in
`Geolocate.scala`, near the beginning (`class KMLParameters`).

For example: For the Twitter corpus, running on different levels of the
document threshold for discarding words, and for the four words "cool",
"coo", "kool" and "kewl", the following code plots the distribution of
each of the words across a cell of degree size 1x1. `--mts=300` is
more for debugging and stops loading further data for generating the
distribution after 300 seconds (5 minutes) has passed.  It's unnecessary
here but may be useful if you have an enormous amount of data (e.g. all
of Wikipedia).

{{{
for x in 0 5 40; do tg-geolocate geotext --doc-thresh $x --mts=300 --degrees-per-cell=1 --mode=generate-kml --kml-words='cool,coo,kool,kewl' --kml-prefix=kml-dist.$x.none. --kml-transform=none; done 
}}}

Another example, just for the words "cool" and "coo", but with different
kinds of transformation of the probabilities.

{{{
for x in none log logsquared; do tg-geolocate geotext --doc-thresh 5 --mts=300 --degrees-per-cell=1 --mode=generate-kml --kml-words='cool,coo' --kml-prefix=kml-dist.5.$x. --kml-transform=$x; done 
}}}

=== Generating data by preprocessing Wikipedia dump files ===

Scripts were written to extract data from raw Wikipedia dump files
and from Twitter, and output in the "TextGrounder corpus format"
required above for `textgrounder geolocate`.

*NOTE*: See `README.preprocess` for detailed instructions on how to
preprocess a raw Wikipedia dump to generate the TextGrounder-format corpora.

*NOTE*: Parsing raw Wikipedia dump files is not easy.  Perhaps better
would have been to download and run the MediaWiki software that generates
the HTML that is actually output.  As it is, there may be occasional
errors in processing.  For example, the code that locates the geotagged
coordinate from an article uses various heuristics to locate the coordinate
from various templates which might specify it, and other heuristics to
fetch the correct coordinate if there is more than one.  In some cases,
this will fetch the correct coordinate even if the MediaWiki software
fails to find it (due to slightly incorrect formatting in the article);
but in other cases, it may find a spurious coordinate. (This happens
particularly for articles that mention a coordinate but don't happen to
be themselves tagged with a coordinate, or when two coordinates are
mentioned in an article. FIXME: We make things worse here by picking the
first coordinate and ignoring `display=title`.  See comments in
`get_coord()` about how to fix this.)

The main script to extract data from a Wikipedia dump is
`python/processwiki.py`.  It takes the (unzipped) dump file as stdin and
processes it according to command-line arguments.  Normally the front
end `python/run-processwiki` is run instead.

To run `python/run-processwiki`, specify steps to do.  Each step generates
one file.  As a shorthand, `all` does all the necessary steps to generate
the Wikipedia data files (but does not generate every possible file that
can be generated).  If you have your own dump file, change the name in
`config-geolocate`.

=== Preprocessing of Twitter ===

There's a script `python/run-process-geotext` used to preprocess the GeoText
corpus of Eisenstein et al. (2010) into a TextGrounder corpus.  This is a
front end for `python/twitter_geotext_process.py` and runs similarly to
the Wikipedia preprocessing code above.

Preprocessing of raw Twitter tweets in JSON format (as are obtained using
the Twitter Streaming API) is done using Scoobi
(https://github.com/nicta/scoobi).  Scoobi is a high-level framework built on
top of Hadoop that lets you do functional-programming-style data processing
(mapping, filtering, etc. of lists) almost exactly as if the data was stored
as lists in local memory, but automatically converts the operations under the
hood into MapReduce steps.

An example of a preprocessing file is `TwitterPull.scala` (badly named),
which processes JSON-format tweets into a TextGrounder corpus, grouping tweets
by user and combining them all into a single document.  The framework for
doing this isn't by any means as developed or automated as the framework
for processing Wikipedia.  For example, `TwitterPull.scala` generates a
single data file in (more or less) the correct TextGrounder corpus format,
but the file isn't named in a way that TextGrounder will recognize it, and
the corresponding schema file is nonexistent and needs to be created by hand.

In general, code written using Scoobi should "just work", and require litle
or no more effort to get working than anything else that uses Hadoop.
Specifically, you have to compile the code, build an assembly exactly as you
would do for running other Hadoop code (see above), copy the data into HDFS,
and run.  For example, you can run `TwitterPull.scala` as follows using
the TextGrounder front end:

$ textgrounder --hadoop run opennlp.textgrounder.preprocess.TwitterPull input output

or you can run it directly using `hadoop`:

$ hadoop jar $TEXTGROUNDER_DIR/target/textgrounder-assembly.jar opennlp.textgrounder.preprocess.TwitterPull input output

In both cases, it is assumed that the data to be preprocessed is located in
the HDFS directory `input` and the results are stored into the HDFS directory
`output`.  All the files in the `input` directory will be logically
concatenated and then processed; in the case of `TwitterPull`, they should be
text files containing JSON-format tweets.

Files that are compressed using GZIP or BZIP2 will automatically be
decompressed.  Note that decompression will take some time, especially of
BZIP2, and may in fact be the dominating factor in the processing time.
Furthermore, GZIP files can't be split by Hadoop, and BZIP2 files can't be
split unless you're using Hadoop 0.21 or later, and Hadoop will choke on
combined BZIP2 files created by concatenating multiple individual BZIP2 files
unless you're using Hadoop 0.21 or later.  Hence unless splitting is possible,
you should limit the size of each input file to about 1 GB uncompressed, or
about 100 MB compressed; that's maybe 24 hours worth of Spritzer downloading
as of June 2012.

There's a script called `twitter-pull` inside of the Twools package
(https://bitbucket.org/utcompling/twools), which can be used for downloading
tweets from Twitter.  This automatically handles retrying after errors along
with exponential backoff (absolutely necessary when doing Twitter streaming,
or you will get locked out temporarily), and starts a new file after a
certain time period (by default, one day), to keep the resulting BZIP-ped
files from getting overly large, as described above.

You should also be able to run in non-distributed mode by using the option
`--hadoop-nondist` in place of `--hadoop`, e.g.:

$ textgrounder --hadoop-nondist run opennlp.textgrounder.preprocess.TwitterPull input output

or you can run it directly using `java`:

$ java -cp $TEXTGROUNDER_DIR/target/textgrounder-assembly.jar opennlp.textgrounder.preprocess.TwitterPull input output

In both cases, `input` and `output` will refer to subdirectories on the local
file system instead of in HDFS.

If this procedure doesn't work and the problem appears to be in the JAR
assembly, an alternative procedure to build this assembly is to use the
Scoobi-specific `package-hadoop` extension to SBT.  Unfortunately, this is
incompatible with `sbt-assembly` (used for building assemblies the normal way),
and hence it can't be enabled by default; a little bit of hacking of the
build procedure is required.  Specifically:

1. Move the file `project/scoobi.scala` to `project/project/scoobi.scala`.
2. Edit the file `build.sbt`, which contains the SBT build instructions,
   and comment out the portions that refer to `sbt-assembly` (otherwise you
   will get a syntax error when trying to run SBT).  This specifically is
   the first line (an import statement for `AssemblyKeys`), as well as a
   section of lines at the end beginning with `seq(assemblySettings ...)`.
3. Then you should be able to build a JAR assembly using `sbt package-hadoop`.
   This will also be in the `target` subdirectory but have a name something
   like `TextGrounder-hadoop-0.3.0.jar`.
4. You will then have to run this JAR using the "direct" methods above
   (not using the `textgrounder` wrapper, because it will attempt to use
   the JAR named `textgrounder-assembly.jar`, as built by `sbt-assembly`).


===========
Bug Reports
===========

Please report bugs by sending mail to jbaldrid at mail.utexas.edu.


============
Special Note
============

Small parts of this README and some of the directory structure and
the build system for this project were borrowed from the JDOM project
(kudos!).  See www.jdom.org for more details.

