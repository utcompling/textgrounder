///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////

////////
//////// Geolocate.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate
import tgutil._
import WordDist.memoizer._
import WordDist.SmoothedWordDist
import argparser._
import Distances._
import Debug._
import GeolocateDriver.Args

import util.matching.Regex
import util.Random
import math._
import collection.mutable
import util.control.Breaks._
import java.io._

/////////////////////////////////////////////////////////////////////////////
//                              Documentation                              //
/////////////////////////////////////////////////////////////////////////////

/*

=== Introduction ===

This program does geolocation (i.e. identification of the geographic
location -- as a pair of latitude/longitude coordinates, somewhere on the
Earth -- either of individual toponyms in a document or of the entire
document).  It uses a statistical model derived from a large corpus
of already geolocated documents, such as Wikipedia -- i.e. it is
supervised (at least in some sense).

The software implements the experiments described in the following paper:

Benjamin Wing and Jason Baldridge (2011), "Simple Supervised Document
Geolocation with Geodesic Grids." In Proceedings of the 49th Annual
Meeting of the Association for Computational Linguistics: Human Language
Technologies, Portland, Oregon, USA, June 2011.

(See http://www.jasonbaldridge.com/papers/wing-baldridge-acl2011.pdf.)

There are three main apps, each of which does a different task:

1. Document geolocation.  This identifies the location of a document.
   Training comes from "articles", which currently are described simply
   by (smoothed) unigram distributions, i.e. counts of the words seen
   in the article.  In addition, each article optionally can be tagged
   with a location (specified as a pair of latitude/longitude values),
   used for training and evaluation; some other per-article data exists
   as well, much of it currently underused or not used at all.  The
   articles themselves are often Wikipedia articles, but the source data
   can come from anywhere (e.g. Twitter feeds, i.e. concatenation of all
   tweets from a given user).  Evaluation is either on other articles
   (with each article treated as a document) or on documents from some
   other source, e.g. chapters from books stored in PCL-Travel format.

2. Toponym geolocation.  This disambiguates each toponym in a document,
   where a toponym is a word such as "London" or "Georgia" that refers
   to a geographic entity such as a city, state, province or country.
   A statistical model is created from article data, as above, but a
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

=== Obtaining the Data ===

If you don't have the data already (you do if you have access to the Comp
Ling machines), download and unzip the processed Wikipedia/Twitter data and
aux files from http://wing.best.vwh.net/wikigrounder.

There are three sets of data to download:
  * The processed Wikipedia data, in `wikipedia/`.  The files are
    listed separately here and bzipped, in case you don't want them all.
    If you're not sure, get them all; or read down below to see which ones
    you need.
  * Auxiliary files, in `wikigrounder-aux-1.0.tar.bz2`. NOTE: Currently the
    only auxiliary file you need is the World Gazetteer, and that is needed
    only when doing toponym resolution.
  * The processed Twitter data, in `wikigrounder-twitter-1.0.tar.bz2`.

Untar these files somewhere.  Then set the following environment variables:
  * `TG_WIKIPEDIA_DIR` points to the directory containing the Wikipedia data.
  * `TG_TWITTER_DIR` points to the directory containing the Twitter data.
  * `TG_AUX_DIR` points to the directory containing the auxiliary data.

(Alternatively, if you are running on a UT NLP machine, or a machine with
a copy of the relevant portions of /groups/corpora and /groups/projects in
the same places, set TG_ON_COMP_LING_MACHINES and it will initialize those
three for you.)

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
    recognized: `wikipedia`, `twitter`, and `twitter-wiki`, which is a
    combination of both corpora).  This sets up additional arguments to
    specify the data files for the corpus/corpora to be loaded/evaluated.
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
the Wikipedia corpus, using KL divergence as a strategy, with a grid size
of 1 degrees.  Options you may find useful (which also apply to
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
paper), `num-articles`  ("??" in the paper), and `random` ("Random" in
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

Data is specified in two main files, given with the options
`--article-data-file` and `--counts-file`.  The article data file lists
all of the articles to be processed and includes various pieces of data
for each article, e.g. title, latitude/longitude coordinates, split
(training, dev, test), number of incoming links (i.e. how many times is
there a link to this article), etc.  The counts file gives word counts
for each article, i.e. which word types occur in the article and how
many times. (Note, the term "article" is used because of the focus on
Wikipedia; but it should be seen as equivalent to "document".)

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

The article data file is formatted as a simple database.  Each line is
an entry (i.e. an article) and consists of a fixed number of fields,
each separated by a tab character.  The first line gives the names of
the fields.  Fields are accessed by name; hence, rearranging fields, and
in most cases, omitting fields, is not a problem as long as the field
names are correct.  The following is a list of the defined fields:

  * `id`: The numeric ID of the article.  Can be arbitrary and currently
  used only when printing out articles.  For Wikipedia articles, this
  corresponds to the internally-assigned ID.

  * `title`: Title of the article.  Must be unique, and must be given
  since it used to look up articles in the counts file.

  * `split`: One of the strings "training", "dev", "test".  Must be
  given.

  * `redir`: If this article is a Wikipedia redirect article, this
  specifies the title of the article redirected to; otherwise, blank.
  This field is not much used by the document-geotagging code (it is
  more important during toponym geotagging).  Its main use in document
  geotagging is in computing the incoming link count of an article (see
  below).

  * `namespace`: The Wikipedia namespace of the article.  Articles not
  in the `Main` namespace have the namespace attached to the beginning
  of the article name, followed by a colon (but not all articles with a
  colon in them have a namespace prefix).  The main significance of this
  field is that articles not in the `Main` namespace are ignored.  However,
  this field can be omitted entirely, in which case all articles are
  assumed to be in `Main`.

  * `is_list_of`, `is_disambig`, `is_list`: These fields should either
  have  the value of "yes" or "no".  These are Wikipedia-specific fields
  (identifying, respectively, whether the article title is "List of ...";
  whether the article is a Wikipedia "disambiguation" page; and whether
  the article is a list of any type, which includes the previous two
  categories as well as some others).  None of these fields are currently
  used.

  * `coord`: Coordinates of an article, or blank.  If specified, the
  format is two floats separated by a comma, giving latitude and longitude,
  respectively (positive for north and east, negative for south and
  west).

  * `incoming_links`: Number of incoming links, or blank if unknown.
  This specifies the number of links pointing to the article from anywhere
  within Wikipedia.  This is primarily used as part of certain baselines
  (`internal-link` and `link-most-common-toponym`).  Note that the actual
  incoming link count of an article includes the incoming link counts
  of any redirects to that article.

The format of the counts file is like this:

{{{
Article title: Ampasimboraka
Article ID: 17242049
population = 16
area = 15
of = 10
mi = 10
sq = 10
leader = 9
density = 8
km2 = 8
image = 8
blank1 = 8
metro = 5
blank = 5
urban = 5
Madagascar = 5
}}}

Multiple articles should follow one directly after the other, with no
blank lines.  The article ID is currently ignored entirely.  There is
no need for the words to be sorted by count (or in any other way); this
is simply done here for ease in debugging.  Note also that, although the
code that generates word counts currently ensures that no word has a
space in it, spaces in general are not a problem, as the code that parses
the word counts specifically looks for an equal sign surrounded by spaces
and followed by a number (hence a "word" containing such a sequence would
be problematic).

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
for x in 0 5 40; do geolocate-twitter --doc-thresh $x --mts=300 --degrees-per-cell=1 --mode=generate-kml --kml-words='cool,coo,kool,kewl' --kml-prefix=kml-dist.$x.none. --kml-transform=none; done 
}}}

Another example, just for the words "cool" and "coo", but with different
kinds of transformation of the probabilities.

{{{
for x in none log logsquared; do geolocate-twitter --doc-thresh 5 --mts=300 --degrees-per-cell=1 --mode=generate-kml --kml-words='cool,coo' --kml-prefix=kml-dist.5.$x. --kml-transform=$x; done 
}}}

=== Generating data ===

Scripts were written to extract data from the raw Wikipedia dump files
and from the Twitter corpus and output in the format required above for
`textgrounder geolocate`.

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

Similarly, to generate Twitter data, use `python/run-process-twitter`, which
is a front end for `python/twitter_geotext_process.py`.

FIXME: Document more.

*/

/////////////////////////////////////////////////////////////////////////////
//                               Structures                                //
/////////////////////////////////////////////////////////////////////////////

//  def print_structure(struct: Any, indent: Int=0) {
//    val indstr = " "*indent
//    if (struct == null)
//      errprint("%snull", indstr)
//    else if (struct.isInstanceOf[Tuple2[Any,Any]]) {
//      val (x,y) = struct.asInstanceOf[Tuple2[Any,Any]]
//      print_structure(List(x,y), indent)
//    } else if (!(struct.isInstanceOf[Seq[Any]]) ||
//               struct.asInstanceOf[Seq[Any]].length == 0)
//      errprint("%s%s", indstr, struct)
//    else {
//      if (struct(0).isInstanceOf[String]) {
//        errprint("%s%s:", indstr, struct.asInstanceOf[String](0))
//        indstr += "  "
//        indent += 2
//        struct = struct.slice(1)
//      }
//      for (s <- struct) {
//        if (isinstance(s, Seq))
//          print_structure(s, indent + 2)
//        else if (isinstance(s, tuple)) {
//          val (key, value) = s
//          if (isinstance(value, Seq)) {
//            errprint("%s%s:", indstr, key)
//            print_structure(value, indent + 2)
//          }
//          else
//            errprint("%s%s: %s", indstr, key, value)
//        }
//        else
//          errprint("%s%s", indstr, s)
//      }
//    }
//  }

class KMLParameters {
  // Minimum and maximum colors
  // FIXME: Allow these to be specified by command-line options
  val kml_mincolor = Array(255.0, 255.0, 0.0) // yellow
  val kml_maxcolor = Array(255.0, 0.0, 0.0) // red

  var kml_max_height: Double = _

  var kml_transform: String = _
}

/////////////////////////////////////////////////////////////////////////////
//                             Word distributions                          //
/////////////////////////////////////////////////////////////////////////////

/**
 * Distribution over words corresponding to a cell.
 */

class CellWordDist extends SmoothedWordDist(
  Array[Word](), Array[Int](), 0, note_globally = false) {
  /** Number of articles included in incoming-link computation. */
  var num_arts_for_links = 0
  /** Total number of incoming links. */
  var incoming_links = 0
  /** Number of articles included in word distribution. */
  var num_arts_for_word_dist = 0

  def is_empty_for_word_dist() = num_arts_for_word_dist == 0

  def is_empty() = num_arts_for_links == 0

  /**
   *  Add the given article to the total distribution seen so far
   */
  def add_article(art: GeoArticle) {
    /* We are passed in all articles, regardless of the split.
       The decision was made to accumulate link counts from all articles,
       even in the evaluation set.  Strictly, this is a violation of the
       "don't train on your evaluation set" rule.  The reason we do this
       is that

       (1) The links are used only in Naive Bayes, and only in establishing
       a prior probability.  Hence they aren't the main indicator.
       (2) Often, nearly all the link count for a given cell comes from
       a particular article -- e.g. the Wikipedia article for the primary
       city in the cell.  If we pull the link count for this article
       out of the cell because it happens to be in the evaluation set,
       we will totally distort the link count for this cell.  In a "real"
       usage case, we would be testing against an unknown article, not
       against an article in our training set that we've artificially
       removed so as to construct an evaluation set, and this problem
       wouldn't arise, so by doing this we are doing a more realistic
       evaluation.
       
       Note that we do NOT include word counts from dev-set or test-set
       articles in the word distribution for a cell.  This keeps to the
       above rule about only training on your training set, and is OK
       because (1) each article in a cell contributes a similar amount of
       word counts (assuming the articles are somewhat similar in size),
       hence in a cell with multiple articles, each individual article
       only computes a fairly small fraction of the total word counts;
       (2) distributions are normalized in any case, so the exact number
       of articles in a cell does not affect the distribution. */
    /* Add link count of article to cell. */
    art.incoming_links match {
      // Might be None, for unknown link count
      case Some(x) => incoming_links += x
      case _ =>
    }
    num_arts_for_links += 1

    /* Add word counts of article to cell, but only if in the
       training set. */
    if (art.split == "training") {
      if (art.dist == null) {
        if (Args.max_time_per_stage == 0.0 && Args.num_training_docs == 0)
          warning("Saw article %s without distribution", art)
      } else {
        assert(art.dist.finished)
        add_word_distribution(art.dist)
        num_arts_for_word_dist += 1
      }
    }
  }

  override def finish(minimum_word_count: Int = 0) {
    super.finish(minimum_word_count = minimum_word_count)

    if (debug("lots")) {
      errprint("""For cell dist, num articles = %s, total tokens = %s,
    unseen_mass = %s, incoming links = %s, overall unseen mass = %s""",
        num_arts_for_word_dist, num_word_tokens,
        unseen_mass, incoming_links,
        overall_unseen_mass)
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Cell distributions                          //
/////////////////////////////////////////////////////////////////////////////

/** A simple distribution associating a probability with each cell. */

class CellDist(
  val cellprobs: mutable.Map[GeoCell, Double]) {
  def get_ranked_cells() = {
    // sort by second element of tuple, in reverse order
    cellprobs.toSeq sortWith (_._2 > _._2)
  }
}

/**
 * Distribution over cells, as might be attached to a word.  If we have a
 *  set of cells, each with a word distribution, then we can imagine
 *  conceptually inverting the process to generate a cell distribution over
 *  words.  Basically, for a given word, look to see what its probability is
 *  in all cells; normalize, and we have a cell distribution.
 *
 *  @param word Word for which the cell is computed
 *  @param cellprobs Hash table listing probabilities associated with cells
 */

class WordCellDist(
  val cellgrid: CellGrid,
  val word: Word
) extends CellDist(mutable.Map[GeoCell, Double]()) {
  var normalized = false

  protected def init() {
    // It's expensive to compute the value for a given word so we cache word
    // distributions.
    var totalprob = 0.0
    // Compute and store un-normalized probabilities for all cells
    for (cell <- cellgrid.iter_nonempty_cells(nonempty_word_dist = true)) {
      val prob = cell.worddist.lookup_word(word)
      // Another way of handling zero probabilities.
      /// Zero probabilities are just a bad idea.  They lead to all sorts of
      /// pathologies when trying to do things like "normalize".
      //if (prob == 0.0)
      //  prob = 1e-50
      cellprobs(cell) = prob
      totalprob += prob
    }
    // Normalize the probabilities; but if all probabilities are 0, then
    // we can't normalize, so leave as-is. (FIXME When can this happen?
    // It does happen when you use --mode=generate-kml and specify words
    // that aren't seen.  In other circumstances, the smoothing ought to
    // ensure that 0 probabilities don't exist?  Anything else I missed?)
    if (totalprob != 0) {
      normalized = true
      for ((cell, prob) <- cellprobs)
        cellprobs(cell) /= totalprob
    } else
      normalized = false
  }

  init()

  // Convert cell to a KML file showing the distribution
  def generate_kml_file(filename: String, params: KMLParameters) {
    val xform = if (params.kml_transform == "log") (x: Double) => log(x)
    else if (params.kml_transform == "logsquared") (x: Double) => -log(x) * log(x)
    else (x: Double) => x

    val xf_minprob = xform(cellprobs.values min)
    val xf_maxprob = xform(cellprobs.values max)

    def yield_cell_kml() = {
      for {
        (cell, prob) <- cellprobs
        kml <- cell.generate_kml(xform(prob), xf_minprob, xf_maxprob, params)
        expr <- kml
      } yield expr
    }

    val allcellkml = yield_cell_kml()

    val kml =
      <kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
        <Document>
          <Style id="bar">
            <PolyStyle>
              <outline>0</outline>
            </PolyStyle>
            <IconStyle>
              <Icon/>
            </IconStyle>
          </Style>
          <Style id="downArrowIcon">
            <IconStyle>
              <Icon>
                <href>http://maps.google.com/mapfiles/kml/pal4/icon28.png</href>
              </Icon>
            </IconStyle>
          </Style>
          <Folder>
            <name>{ unmemoize_word(word) }</name>
            <open>1</open>
            <description>{ "Cell distribution for word '%s'" format unmemoize_word(word) }</description>
            <LookAt>
              <latitude>42</latitude>
              <longitude>-102</longitude>
              <altitude>0</altitude>
              <range>5000000</range>
              <tilt>53.454348562403</tilt>
              <heading>0</heading>
            </LookAt>
            { allcellkml }
          </Folder>
        </Document>
      </kml>

    xml.XML.save(filename, kml)
  }
}

class CellDistFactory(val lru_cache_size: Int) {
  var cached_dists: LRUCache[Word, WordCellDist] = null

  // Return a cell distribution over a given word, using a least-recently-used
  // cache to optimize access.
  def get_cell_dist(cellgrid: CellGrid, word: Word) = {
    if (cached_dists == null)
      cached_dists = new LRUCache(maxsize = lru_cache_size)
    cached_dists.get(word) match {
      case Some(dist) => dist
      case None => {
        val dist = new WordCellDist(cellgrid, word)
        cached_dists(word) = dist
        dist
      }
    }
  }

  /**
   * Return a cell distribution over a distribution over words.  This works
   * by adding up the distributions of the individual words, weighting by
   * the count of the each word.
   */
  def get_cell_dist_for_word_dist(cellgrid: CellGrid, xworddist: WordDist) = {
    // FIXME!!! Figure out what to do if distribution is not a unigram dist.
    // Can we break this up into smaller operations?  Or do we have to
    // make it an interface for WordDist?
    val worddist = xworddist.asInstanceOf[UnigramWordDist]
    val cellprobs = doublemap[GeoCell]()
    for ((word, count) <- worddist.counts) {
      val dist = get_cell_dist(cellgrid, word)
      for ((cell, prob) <- dist.cellprobs)
        cellprobs(cell) += count * prob
    }
    val totalprob = (cellprobs.values sum)
    for ((cell, prob) <- cellprobs)
      cellprobs(cell) /= totalprob
    new CellDist(cellprobs)
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Cells in a grid                             //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for a general cell in a cell grid.
 * 
 * @param cellgrid The CellGrid object for the grid this cell is in.
 */
abstract class GeoCell(val cellgrid: CellGrid) {
  val worddist = new CellWordDist()
  var most_popular_article: GeoArticle = null
  var mostpopart_links = 0

  /**
   * Return a string describing the location of the cell in its grid,
   * e.g. by its boundaries or similar.
   */
  def describe_location(): String

  /**
   * Return a string describing the indices of the cell in its grid.
   * Only used for debugging.
   */
  def describe_indices(): String

  /**
   * Return an Iterable over articles, listing the articles in the cell.
   */
  def iterate_articles(): Iterable[GeoArticle]

  /**
   * Return the coordinate of the "center" of the cell.  This is the
   * coordinate used in computing distances between arbitary points and
   * given cells, for evaluation and such.  For odd-shaped cells, the
   * center can be more or less arbitrarily placed as long as it's somewhere
   * central.
   */
  def get_center_coord(): Coord

  /**
   * Generate KML for a single cell.
   */
  def generate_kml(xfprob: Double, xf_minprob: Double, xf_maxprob: Double,
    params: KMLParameters): Iterable[xml.Elem]

  /**
   * Return a string representation of the cell.  Generally does not need
   * to be overridden.
   */
  override def toString() = {
    val unfinished = if (worddist.finished) "" else ", unfinished"
    val contains =
      if (most_popular_article != null)
        ", most-pop-art %s(%d links)" format (
          most_popular_article, mostpopart_links)
      else ""

    "GeoCell(%s%s%s, %d articles(dist), %d articles(links), %d links)" format (
      describe_location(), unfinished, contains,
      worddist.num_arts_for_word_dist, worddist.num_arts_for_links,
      worddist.incoming_links)
  }

  // def __repr__() = {
  //   toString.encode("utf-8")
  // }

  /**
   * Return a shorter string representation of the cell, for
   * logging purposes.
   */
  def shortstr() = {
    var str = "Cell %s" format describe_location()
    val mostpop = most_popular_article
    if (mostpop != null)
      str += ", most-popular %s" format mostpop.shortstr()
    str
  }

  /**
   * Return an XML representation of the cell.  Currently used only for
   * debugging-output purposes, so the exact representation isn't too important.
   */
  def struct() =
    <GeoCell>
      <bounds>{ describe_location() }</bounds>
      <finished>{ worddist.finished }</finished>
      {
        if (most_popular_article != null)
          (<mostPopularArticle>most_popular_article.struct()</mostPopularArticle>
           <mostPopularArticleLinks>mostpopart_links</mostPopularArticleLinks>)
      }
      <numArticlesDist>{ worddist.num_arts_for_word_dist }</numArticlesDist>
      <numArticlesLink>{ worddist.num_arts_for_links }</numArticlesLink>
      <incomingLinks>{ worddist.incoming_links }</incomingLinks>
    </GeoCell>

  /**
   * Generate the distribution for a cell from the articles in it.
   */
  def generate_dist() {
    for (art <- iterate_articles()) {
      worddist.add_article(art)
      if (art.incoming_links != None &&
        art.incoming_links.get > mostpopart_links) {
        mostpopart_links = art.incoming_links.get
        most_popular_article = art
      }
    }
    worddist.finish(minimum_word_count = Args.minimum_word_count)
  }
}

/**
 * A cell in a polygonal shape.
 *
 * @param cellgrid The CellGrid object for the grid this cell is in.
 */
abstract class PolygonalCell(
  cellgrid: CellGrid) extends GeoCell(cellgrid) {
  /**
   * Return the boundary of the cell as an Iterable of coordinates, tracing
   * out the boundary vertex by vertex.  The last coordinate should be the
   * same as the first, as befits a closed shape.
   */
  def get_boundary(): Iterable[Coord]

  /**
   * Return the "inner boundary" -- something echoing the actual boundary of the
   * cell but with smaller dimensions.  Used for outputting KML to make the
   * output easier to read.
   */
  def get_inner_boundary() = {
    val center = get_center_coord()
    for (coord <- get_boundary())
      yield Coord((center.lat + coord.lat) / 2.0,
                  average_longitudes(center.long, coord.long))
  }

  /**
   * Generate the KML placemark for the cell's name.  Currently it's rectangular
   * for rectangular cells.  FIXME: Perhaps it should be generalized so it doesn't
   * need to be redefined for differently-shaped cells.
   *
   * @param name The name to display in the placemark
   */
  def generate_kml_name_placemark(name: String): xml.Elem

  def generate_kml(xfprob: Double, xf_minprob: Double, xf_maxprob: Double,
      params: KMLParameters) = {
    val offprob = xfprob - xf_minprob
    val fracprob = offprob / (xf_maxprob - xf_minprob)
    var coordtext = "\n"
    for (coord <- get_inner_boundary()) {
      coordtext += "%s,%s,%s\n" format (
        coord.long, coord.lat, fracprob * params.kml_max_height)
    }
    val name =
      if (most_popular_article != null) most_popular_article.title
      else ""

    // Placemark indicating name
    val name_placemark = generate_kml_name_placemark(name)

    // Interpolate colors
    val color = Array(0.0, 0.0, 0.0)
    for (i <- 0 until 3) {
      color(i) = (params.kml_mincolor(i) +
        fracprob * (params.kml_maxcolor(i) - params.kml_mincolor(i)))
    }
    // Original color dc0155ff
    //rgbcolor = "dc0155ff"
    val revcol = color.reverse
    val rgbcolor = "ff%02x%02x%02x" format (
      revcol(0).toInt, revcol(1).toInt, revcol(2).toInt)

    // Yield cylinder indicating probability by height and color

    // !!PY2SCALA: BEGIN_PASSTHRU
    val cylinder_placemark =
      <Placemark>
        <name>{ "%s POLYGON" format name }</name>
        <styleUrl>#bar</styleUrl>
        <Style>
          <PolyStyle>
            <color>{ rgbcolor }</color>
            <colorMode>normal</colorMode>
          </PolyStyle>
        </Style>
        <Polygon>
          <extrude>1</extrude>
          <tessellate>1</tessellate>
          <altitudeMode>relativeToGround</altitudeMode>
          <outerBoundaryIs>
            <LinearRing>
              <coordinates>{ coordtext }</coordinates>
            </LinearRing>
          </outerBoundaryIs>
        </Polygon>
      </Placemark>
    // !!PY2SCALA: END_PASSTHRU
    Seq(name_placemark, cylinder_placemark)
  }
}

/**
 * A cell in a rectangular shape.
 *
 * @param cellgrid The CellGrid object for the grid this cell is in.
 */
abstract class RectangularCell(
  cellgrid: CellGrid
) extends PolygonalCell(cellgrid) {
  /**
   * Return the coordinate of the southwest point of the rectangle.
   */
  def get_southwest_coord(): Coord
  /**
   * Return the coordinate of the northeast point of the rectangle.
   */
  def get_northeast_coord(): Coord
  /**
   * Define the center based on the southwest and northeast points.
   */
  def get_center_coord() = {
    val sw = get_southwest_coord()
    val ne = get_northeast_coord()
    Coord((sw.lat + ne.lat) / 2.0, (sw.long + ne.long) / 2.0)
  }

  /**
   * Define the boundary given the specified southwest and northeast
   * points.
   */
  def get_boundary() = {
    val sw = get_southwest_coord()
    val ne = get_northeast_coord()
    val center = get_center_coord()
    val nw = Coord(ne.lat, sw.long)
    val se = Coord(sw.lat, ne.long)
    Seq(sw, nw, ne, se, sw)
  }

  /**
   * Generate the name placemark as a smaller rectangle within the
   * larger rectangle. (FIXME: Currently it is exactly the size of
   * the inner boundary.  Perhaps this should be generalized, so
   * that the definition of this function can be handled up at the
   * polygonal-shaped-cell level.)
   */
  def generate_kml_name_placemark(name: String) = {
    val sw = get_southwest_coord()
    val ne = get_northeast_coord()
    val center = get_center_coord()
    // !!PY2SCALA: BEGIN_PASSTHRU
    // Because it tries to frob the # sign
    <Placemark>
      <name>{ name }</name>
      ,
      <Cell>
        <LatLonAltBox>
          <north>{ ((center.lat + ne.lat) / 2).toString }</north>
          <south>{ ((center.lat + sw.lat) / 2).toString }</south>
          <east>{ ((center.long + ne.long) / 2).toString }</east>
          <west>{ ((center.long + sw.long) / 2).toString }</west>
        </LatLonAltBox>
        <Lod>
          <minLodPixels>16</minLodPixels>
        </Lod>
      </Cell>
      <styleURL>#bar</styleURL>
      <Point>
        <coordinates>{ "%s,%s" format (center.long, center.lat) }</coordinates>
      </Point>
    </Placemark>
    // !!PY2SCALA: END_PASSTHRU
  }
}

/**
 * Abstract class for a grid of cells covering the earth.
 */
abstract class CellGrid(val table: GeoArticleTable) {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null.
   */
  def find_best_cell_for_coord(coord: Coord): GeoCell

  /**
   * Add the given article to the cell grid.
   */
  def add_article_to_cell(article: GeoArticle): Unit

  /**
   * Generate all non-empty cells.  This will be called once (and only once),
   * after all articles have been added to the cell grid by calling
   * `add_article_to_cell`.  The generation happens internally; but after
   * this, `iter_nonempty_cells` should work properly.  This is not meant
   * to be called externally.
   */
  protected def initialize_cells(): Unit

  /**
   * Iterate over all non-empty cells.
   * 
   * @param nonempty_word_dist If given, returned cells must also have a
   *   non-empty word distribution; otherwise, they just need to have at least
   *   one article in them. (Not all articles have word distributions, esp.
   *   when --max-time-per-stage has been set to a non-zero value so that we
   *   only load some subset of the word distributions for all articles.  But
   *   even when not set, some articles may be listed in the article-data file
   *   but have no corresponding word counts given in the counts file.)
   */
  def iter_nonempty_cells(nonempty_word_dist: Boolean = false): Iterable[GeoCell]
  
  /*********************** Not meant to be overridden *********************/
  
  /* These are simply the sum of the corresponding counts
     `num_arts_for_word_dist` and `num_arts_for_links` of each individual
     cell. */
  var total_num_arts_for_word_dist = 0
  var total_num_arts_for_links = 0
  /* Set once finish() is called. */
  var all_cells_computed = false
  /* Number of non-empty cells. */
  var num_non_empty_cells = 0

  /**
   * This function is called externally to initialize the cells.  It is a
   * wrapper around `initialize_cells()`, which is not meant to be called
   * externally.  Normally this does not need to be overridden.
   */
  def finish() {
    assert(!all_cells_computed)

    initialize_cells()

    all_cells_computed = true

    total_num_arts_for_links = 0
    total_num_arts_for_word_dist = 0
    for (cell <- iter_nonempty_cells()) {
      total_num_arts_for_word_dist += cell.worddist.num_arts_for_word_dist
      total_num_arts_for_links += cell.worddist.num_arts_for_links
    }

    errprint("Number of non-empty cells: %s", num_non_empty_cells)
    errprint("Total number of cells: %s", total_num_cells)
    errprint("Percent non-empty cells: %g",
      num_non_empty_cells.toDouble / total_num_cells)
    val training_arts_with_word_counts =
      table.num_word_count_articles_by_split("training")
    errprint("Training articles per non-empty cell: %g",
      training_arts_with_word_counts.toDouble / num_non_empty_cells)
    // Clear out the article distributions of the training set, since
    // only needed when computing cells.
    //
    // FIXME: Could perhaps save more memory, or at least total memory used,
    // by never creating these distributions at all, but directly adding
    // them to the cells.  Would require a bit of thinking when reading
    // in the counts.
    table.clear_training_article_distributions()
  }
}

/////////////////////////////////////////////////////////////////////////////
//                         A regularly spaced grid                         //
/////////////////////////////////////////////////////////////////////////////

/* 
  We divide the earth's surface into "tiling cells", all of which are the
  same square size, running on latitude/longitude lines, and which have a
  constant number of degrees on a size, set using the value of the command-
  line option --degrees-per-cell. (Alternatively, the value of --miles-per-cell
  or --km-per-cell are converted into degrees using 'miles_per_degree' or
  'km_per_degree', respectively, which specify the size of a degree at
  the equator and is derived from the value for the Earth's radius.)

  In addition, we form a square of tiling cells in order to create a
  "multi cell", which is used to compute a distribution over words.  The
  number of tiling cells on a side of a multi cell is determined by
  --width-of-multi-cell.  Note that if this is greater than 1, different
  multi cells will overlap.

  To specify a cell, we use cell indices, which are derived from
  coordinates by dividing by degrees_per_cell.  Hence, if for example
  degrees_per_cell is 2.0, then cell indices are in the range [-45,+45]
  for latitude and [-90,+90) for longitude.  In general, an arbitrary
  coordinate will have fractional cell indices; however, the cell indices
  of the corners of a cell (tiling or multi) will be integers.  Normally,
  we use the southwest corner to specify a cell.

  Correspondingly, to convert a cell index to a Coord, we multiply
  latitude and longitude by degrees_per_cell.

  Near the edges, tiling cells may be truncated.  Multi cells will
  wrap around longitudinally, and will still have the same number of
  tiling cells, but may be smaller.
  */

/**
 * The index of a regular cell, using "cell index" integers, as described
 * above.
 */
case class RegularCellIndex(latind: Int, longind: Int) {
  def toFractional() = FractionalRegularCellIndex(latind, longind)
}

/**
 * Similar to `RegularCellIndex`, but for the case where the indices are
 * fractional, representing a location other than at the corners of a
 * cell.
 */
case class FractionalRegularCellIndex(latind: Double, longind: Double) {
}

/**
 * A cell where the cell grid is a MultiRegularCellGrid. (See that class.)
 *
 * @param cellgrid The CellGrid object for the grid this cell is in,
 *   an instance of MultiRegularCellGrid.
 * @param index Index of this cell in the grid
 */

class MultiRegularCell(
  cellgrid: MultiRegularCellGrid,
  val index: RegularCellIndex
) extends RectangularCell(cellgrid) {

  def get_southwest_coord() =
    cellgrid.multi_cell_index_to_near_corner_coord(index)

  def get_northeast_coord() =
    cellgrid.multi_cell_index_to_far_corner_coord(index)

  def describe_location() = {
    "%s-%s" format (get_southwest_coord(), get_northeast_coord())
  }

  def describe_indices() = "%s,%s" format (index.latind, index.longind)

  def iterate_articles() = {
    val maxlatind = (
      (cellgrid.maximum_latind + 1) min
      (index.latind + cellgrid.width_of_multi_cell))

    if (debug("lots")) {
      errprint("Generating distribution for multi cell centered at %s",
        cellgrid.cell_index_to_coord(index))
    }

    // Process the tiling cells making up the multi cell;
    // but be careful around the edges.  Truncate the latitude, wrap the
    // longitude.
    for {
      // The use of view() here in both iterators causes this iterable to
      // be lazy; hence the print statement below doesn't get executed until
      // we actually process the articles in question.
      i <- (index.latind until maxlatind) view;
      rawj <- (index.longind until
        (index.longind + cellgrid.width_of_multi_cell)) view;
      val j = (if (rawj > cellgrid.maximum_longind) rawj - 360 else rawj)
      art <- {
        if (debug("lots")) {
          errprint("--> Processing tiling cell %s",
            cellgrid.cell_index_to_coord(index))
        }
        cellgrid.tiling_cell_to_articles.getNoSet(RegularCellIndex(i, j))
      }
    } yield art
  }
}

/**
 * Grid composed of possibly-overlapping multi cells, based on an underlying
 * grid of regularly-spaced square cells tiling the earth.  The multi cells,
 * over which word distributions are computed for comparison with the word
 * distribution of a given article, are composed of NxN tiles, where possibly
 * N > 1.
 *
 * FIXME: We should abstract out the concept of a grid composed of tiles and
 * a grid composed of overlapping conglomerations of tiles; this could be
 * useful e.g. for KD trees or other representations where we might want to
 * compare with cells at multiple levels of granularity.
 * 
 * @param degrees_per_cell Size of each cell in degrees.  Determined by the
 *   --degrees-per-cell option, unless --miles-per-cell is set, in which
 *   case it takes priority.
 * @param width_of_multi_cell Size of multi cells in tiling cells,
 *   determined by the --width-of-multi-cell option.
 */
class MultiRegularCellGrid(
  val degrees_per_cell: Double,
  val width_of_multi_cell: Int,
  table: GeoArticleTable
) extends CellGrid(table) {

  /**
   * Size of each cell (vertical dimension; horizontal dimension only near
   * the equator) in km.  Determined from degrees_per_cell.
   */
  val km_per_cell = degrees_per_cell * km_per_degree

  /* Set minimum, maximum latitude/longitude in indices (integers used to
     index the set of cells that tile the earth).   The actual maximum
     latitude is exactly 90 (the North Pole).  But if we set degrees per
     cell to be a number that exactly divides 180, and we use
     maximum_latitude = 90 in the following computations, then we would
     end up with the North Pole in a cell by itself, something we probably
     don't want.
   */
  val maximum_index =
    coord_to_tiling_cell_index(Coord(maximum_latitude - 1e-10,
      maximum_longitude))
  val maximum_latind = maximum_index.latind
  val maximum_longind = maximum_index.longind
  val minimum_index =
    coord_to_tiling_cell_index(Coord(minimum_latitude, minimum_longitude))
  val minimum_latind = minimum_index.latind
  val minimum_longind = minimum_index.longind

  /**
   * Mapping of cell->locations in cell, for cell-based Naive Bayes
   * disambiguation.  The key is a tuple expressing the integer indices of the
   * latitude and longitude of the southwest corner of the cell. (Basically,
   * given an index, the latitude or longitude of the southwest corner is
   * index*degrees_per_cell, and the cell includes all locations whose
   * latitude or longitude is in the half-open interval
   * [index*degrees_per_cell, (index+1)*degrees_per_cell).
   *
   * We don't just create an array because we expect many cells to have no
   * articles in them, esp. as we decrease the cell size.  The idea is that
   * the cells provide a first approximation to the cells used to create the
   * article distributions.
   */
  var tiling_cell_to_articles = bufmap[RegularCellIndex, GeoArticle]()

  /**
   * Mapping from index of southwest corner of multi cell to corresponding
   * cell object.  A "multi cell" is made up of a square of tiling cells,
   * with the number of cells on a side determined by `width_of_multi_cell'.
   * A word distribution is associated with each multi cell.
   */
  val corner_to_multi_cell = mutable.Map[RegularCellIndex, MultiRegularCell]()

  var total_num_cells = 0

  /*************** Conversion between Cell indices and Coords *************/

  /* The different functions vary depending on where in the particular cell
     the Coord is wanted, e.g. one of the corners or the center. */

  /**
   * Convert a coordinate to the indices of the southwest corner of the
   * corresponding tiling cell.
   */
  def coord_to_tiling_cell_index(coord: Coord) = {
    val latind = floor(coord.lat / degrees_per_cell).toInt
    val longind = floor(coord.long / degrees_per_cell).toInt
    RegularCellIndex(latind, longind)
  }

  /**
   * Convert a coordinate to the indices of the southwest corner of the
   * corresponding multi cell.
   */
  def coord_to_multi_cell_index(coord: Coord) = {
    // When width_of_multi_cell = 1, don't subtract anything.
    // When width_of_multi_cell = 2, subtract 0.5*degrees_per_cell.
    // When width_of_multi_cell = 3, subtract degrees_per_cell.
    // When width_of_multi_cell = 4, subtract 1.5*degrees_per_cell.
    // In general, subtract (width_of_multi_cell-1)/2.0*degrees_per_cell.

    // Compute the indices of the southwest cell
    val subval = (width_of_multi_cell - 1) / 2.0 * degrees_per_cell
    coord_to_tiling_cell_index(
      Coord(coord.lat - subval, coord.long - subval))
  }

  /**
   * Convert a fractional cell index to the corresponding coordinate.  Useful
   * for indices not referring to the corner of a cell.
   * 
   * @seealso #cell_index_to_coord
   */
  def fractional_cell_index_to_coord(index: FractionalRegularCellIndex,
    method: String = "coerce-warn") = {
    Coord(index.latind * degrees_per_cell, index.longind * degrees_per_cell,
      method)
  }

  /**
   * Convert cell indices to the corresponding coordinate.  This can also
   * be used to find the coordinate of the southwest corner of a tiling cell
   * or multi cell, as both are identified by the cell indices of
   * their southwest corner.
   */
  def cell_index_to_coord(index: RegularCellIndex,
    method: String = "coerce-warn") =
    fractional_cell_index_to_coord(index.toFractional, method)

  /** 
   * Add 'offset' to both latind and longind of 'index' and then convert to a
   * coordinate.  Coerce the coordinate to be within bounds.
   */
  def offset_cell_index_to_coord(index: RegularCellIndex,
    offset: Double) = {
    fractional_cell_index_to_coord(
      FractionalRegularCellIndex(index.latind + offset, index.longind + offset),
      "coerce")
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * near (i.e. southwest) corner of the cell.
   */
  def tiling_cell_index_to_near_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(index)
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * center of the cell.
   */
  def tiling_cell_index_to_center_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, 0.5)
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * far (i.e. northeast) corner of the cell.
   */
  def tiling_cell_index_to_far_corner_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, 1.0)
  }
  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * near (i.e. southwest) corner of the cell.
   */
  def multi_cell_index_to_near_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(index)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * center of the cell.
   */
  def multi_cell_index_to_center_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, width_of_multi_cell / 2.0)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * far (i.e. northeast) corner of the cell.
   */
  def multi_cell_index_to_far_corner_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, width_of_multi_cell)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * northwest corner of the cell.
   */
  def multi_cell_index_to_nw_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(
      RegularCellIndex(index.latind + width_of_multi_cell, index.longind),
      "coerce")
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * southeast corner of the cell.
   */
  def multi_cell_index_to_se_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(
      RegularCellIndex(index.latind, index.longind + width_of_multi_cell),
      "coerce")
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * southwest corner of the cell.
   */
  def multi_cell_index_to_sw_corner_coord(index: RegularCellIndex) = {
    multi_cell_index_to_near_corner_coord(index)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * northeast corner of the cell.
   */
  def multi_cell_index_to_ne_corner_coord(index: RegularCellIndex) = {
    multi_cell_index_to_far_corner_coord(index)
  }

  /*************** End conversion functions *************/

  def find_best_cell_for_coord(coord: Coord) = {
    val index = coord_to_multi_cell_index(coord)
    find_cell_for_cell_index(index)
  }

  protected def find_cell_for_cell_index(index: RegularCellIndex,
    create: Boolean = false) = {
    if (!create)
      assert(all_cells_computed)
    val statcell = corner_to_multi_cell.getOrElse(index, null)
    if (statcell != null)
      statcell
    else if (!create) null
    else {
      val newstat = new MultiRegularCell(this, index)
      newstat.generate_dist()
      if (newstat.worddist.is_empty())
        null
      else {
        num_non_empty_cells += 1
        corner_to_multi_cell(index) = newstat
        newstat
      }
    }
  }

  def add_article_to_cell(article: GeoArticle) {
    val index = coord_to_tiling_cell_index(article.coord)
    tiling_cell_to_articles(index) += article
  }

  protected def initialize_cells() {
    val task = new MeteredTask("Earth-tiling cell", "generating non-empty")

    for (i <- minimum_latind to maximum_latind view) {
      for (j <- minimum_longind to maximum_longind view) {
        total_num_cells += 1
        val cell = find_cell_for_cell_index(RegularCellIndex(i, j),
          create = true)
        if (debug("cell") && !cell.worddist.is_empty)
          errprint("--> (%d,%d): %s", i, j, cell)
        task.item_processed()
      }
    }
    task.finish()

    // Save some memory by clearing this after it's not needed
    tiling_cell_to_articles = null
  }

  def iter_nonempty_cells(nonempty_word_dist: Boolean = false) = {
    assert(all_cells_computed)
    for {
      v <- corner_to_multi_cell.values
      val empty = (
        if (nonempty_word_dist) v.worddist.is_empty_for_word_dist()
        else v.worddist.is_empty())
      if (!empty)
    } yield v
  }

  /**
   * Output a "ranking grid" of information so that a nice 3-D graph
   * can be created showing the ranks of cells surrounding the true
   * cell, out to a certain distance.
   * 
   * @param pred_cells List of predicted cells, along with their scores.
   * @param true_cell True cell.
   * @param grsize Total size of the ranking grid. (For example, a total size
   *   of 21 will result in a ranking grid with the true cell and 10
   *   cells on each side shown.)
   */
  def output_ranking_grid(pred_cells: Seq[(MultiRegularCell, Double)],
    true_cell: MultiRegularCell, grsize: Int) {
    val (true_latind, true_longind) =
      (true_cell.index.latind, true_cell.index.longind)
    val min_latind = true_latind - grsize / 2
    val max_latind = min_latind + grsize - 1
    val min_longind = true_longind - grsize / 2
    val max_longind = min_longind + grsize - 1
    val grid = mutable.Map[RegularCellIndex, (GeoCell, Double, Int)]()
    for (((cell, value), rank) <- pred_cells zip (1 to pred_cells.length)) {
      val (la, lo) = (cell.index.latind, cell.index.longind)
      if (la >= min_latind && la <= max_latind &&
        lo >= min_longind && lo <= max_longind)
        grid(cell.index) = (cell, value, rank)
    }

    errprint("Grid ranking, gridsize %dx%d", grsize, grsize)
    errprint("NW corner: %s",
      multi_cell_index_to_nw_corner_coord(
        RegularCellIndex(max_latind, min_longind)))
    errprint("SE corner: %s",
      multi_cell_index_to_se_corner_coord(
        RegularCellIndex(min_latind, max_longind)))
    for (doit <- Seq(0, 1)) {
      if (doit == 0)
        errprint("Grid for ranking:")
      else
        errprint("Grid for goodness/distance:")
      for (lat <- max_latind to min_latind) {
        for (long <- fromto(min_longind, max_longind)) {
          val cellvalrank = grid.getOrElse(RegularCellIndex(lat, long), null)
          if (cellvalrank == null)
            errout(" %-8s", "empty")
          else {
            val (cell, value, rank) = cellvalrank
            val showit = if (doit == 0) rank else value
            if (lat == true_latind && long == true_longind)
              errout("!%-8.6s", showit)
            else
              errout(" %-8.6s", showit)
          }
        }
        errout("\n")
      }
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
//                         Wikipedia/Twitter articles                      //
/////////////////////////////////////////////////////////////////////////////

//////////////////////  Article table

/**
 * Class maintaining tables listing all articles and mapping between
 * names, ID's and articles.  Objects corresponding to redirect articles
 * should not be present anywhere in this table; instead, the name of the
 * redirect article should point to the article object for the article
 * pointed to by the redirect.
 */
class GeoArticleTable {
  /**
   * Mapping from article names to GeoArticle objects, using the actual case of
   * the article.
   */
  val name_to_article = mutable.Map[String, GeoArticle]()

  /**
   * List of articles in each split.
   */
  val articles_by_split = bufmap[String, GeoArticle]()

  /**
   * Num of articles with word-count information but not in table.
   */
  var num_articles_with_word_counts_but_not_in_table = 0

  /**
   * Num of articles with word-count information (whether or not in table).
   */
  var num_articles_with_word_counts = 0

  /** 
   * Num of articles in each split with word-count information seen.
   */
  val num_word_count_articles_by_split = intmap[String]()

  /**
   * Num of articles in each split with a computed distribution.
   * (Not the same as the previous since we don't compute the distribution of
   * articles in either the test or dev set depending on which one is used.)
   */
  val num_dist_articles_by_split = intmap[String]()

  /**
   * Total # of word tokens for all articles in each split.
   */
  val word_tokens_by_split = intmap[String]()

  /**
   * Total # of incoming links for all articles in each split.
   */
  val incoming_links_by_split = intmap[String]()

  /**
   * Map from short name (lowercased) to list of articles.
   * The short name for an article is computed from the article's name.  If
   * the article name has a comma, the short name is the part before the
   * comma, e.g. the short name of "Springfield, Ohio" is "Springfield".
   * If the name has no comma, the short name is the same as the article
   * name.  The idea is that the short name should be the same as one of
   * the toponyms used to refer to the article.
   */
  val short_lower_name_to_articles = bufmap[String, GeoArticle]()

  /**
   * Map from tuple (NAME, DIV) for articles of the form "Springfield, Ohio",
   * lowercased.
   */
  val lower_name_div_to_articles = bufmap[(String, String), GeoArticle]()

  /**
   * For each toponym, list of articles matching the name.
   */
  val lower_toponym_to_article = bufmap[String, GeoArticle]()

  /**
   * Mapping from lowercased article names to TopoArticle objects
   */
  val lower_name_to_articles = bufmap[String, GeoArticle]()

  /**
   * Look up an article named NAME and return the associated article.
   * Note that article names are case-sensitive but the first letter needs to
   * be capitalized.
   */
  def lookup_article(name: String) = {
    assert(name != null)
    name_to_article.getOrElse(capfirst(name), null)
  }

  /**
   * Record the article as having NAME as one of its names (there may be
   * multiple names, due to redirects).  Also add to related lists mapping
   * lowercased form, short form, etc.
   */ 
  def record_article_name(name: String, art: GeoArticle) {
    // Must pass in properly cased name
    // errprint("name=%s, capfirst=%s", name, capfirst(name))
    // println("length=%s" format name.length)
    // if (name.length > 1) {
    //   println("name(0)=0x%x" format name(0).toInt)
    //   println("name(1)=0x%x" format name(1).toInt)
    //   println("capfirst(0)=0x%x" format capfirst(name)(0).toInt)
    // }
    assert(name == capfirst(name))
    name_to_article(name) = art
    val loname = name.toLowerCase
    lower_name_to_articles(loname) += art
    val (short, div) = Article.compute_short_form(loname)
    if (div != null)
      lower_name_div_to_articles((short, div)) += art
    short_lower_name_to_articles(short) += art
    if (!(lower_toponym_to_article(loname) contains art))
      lower_toponym_to_article(loname) += art
    if (short != loname && !(lower_toponym_to_article(short) contains art))
      lower_toponym_to_article(short) += art
  }

  /**
   * Record either a normal article ('artfrom' same as 'artto') or a
   * redirect ('artfrom' redirects to 'artto').
   */
  def record_article(artfrom: GeoArticle, artto: GeoArticle) {
    record_article_name(artfrom.title, artto)
    val redir = !(artfrom eq artto)
    val split = artto.split
    val fromlinks = artfrom.adjusted_incoming_links
    incoming_links_by_split(split) += fromlinks
    if (!redir) {
      articles_by_split(split) += artto
    } else if (fromlinks != 0) {
      // Add count of links pointing to a redirect to count of links
      // pointing to the article redirected to, so that the total incoming
      // link count of an article includes any redirects to that article.
      artto.incoming_links = Some(artto.adjusted_incoming_links + fromlinks)
    }
  }

  def create_article(params: Map[String, String]) = new GeoArticle(params)

  def read_article_data(filename: String, cellgrid: CellGrid) {
    val redirects = mutable.Buffer[GeoArticle]()

    def process(params: Map[String, String]) {
      val art = create_article(params)
      if (art.namespace != "Main")
        return
      if (art.redir.length > 0)
        redirects += art
      else if (art.coord != null) {
        record_article(art, art)
        cellgrid.add_article_to_cell(art)
      }
    }

    ArticleData.read_article_data_file(filename, process,
      maxtime = Args.max_time_per_stage)

    for (x <- redirects) {
      val redart = lookup_article(x.redir)
      if (redart != null)
        record_article(x, redart)
    }
  }

  def finish_article_distributions() {
    // Figure out the value of OVERALL_UNSEEN_MASS for each article.
    for ((split, table) <- articles_by_split) {
      var totaltoks = 0
      var numarts = 0
      for (art <- table) {
        if (art.dist != null) {
          /* FIXME: Move this finish() earlier, and split into
             before/after global. */
          art.dist.finish(minimum_word_count = Args.minimum_word_count)
          totaltoks += art.dist.num_word_tokens
          numarts += 1
        }
      }
      num_dist_articles_by_split(split) = numarts
      word_tokens_by_split(split) = totaltoks
    }
  }

  def clear_training_article_distributions() {
    for (art <- articles_by_split("training"))
      art.dist = null
  }

  /**
   * Parse the result of a previous run of --output-counts and generate
   * a unigram distribution for Naive Bayes matching.  We do a simple version
   * of Good-Turing smoothing where we assign probability mass to unseen
   * words equal to the probability mass of all words seen once, and rescale
   * the remaining probabilities accordingly.
   */ 
  def read_word_counts(filename: String, stopwords: Set[String]) {
    val initial_dynarr_size = 1000
    val keys_dynarr =
      new DynamicArray[Word](initial_alloc = initial_dynarr_size)
    val values_dynarr =
      new DynamicArray[Int](initial_alloc = initial_dynarr_size)

    // This is basically a one-off debug statement because of the fact that
    // the experiments published in the paper used a word-count file generated
    // using an older algorithm for determining the geotagged coordinate of
    // an article.  We didn't record the corresponding article-data
    // file, so we need a way of regenerating it using the intersection of
    // articles in the article-data file we actually used for the experiments
    // and the word-count file we used.
    var stream: PrintStream = null
    var writer: ArticleWriter = null
    if (debug("wordcountarts")) {
      // Change this if you want a different file name
      val wordcountarts_filename = "wordcountarts-combined-article-data.txt"
      stream = openw(wordcountarts_filename)
      // See write_article_data_file() in ArticleData.scala
      writer =
        new ArticleWriter(stream, ArticleData.combined_article_data_outfields)
      writer.output_header()
    }

    var num_word_tokens = 0
    var title = null: String

    def one_article_probs() {
      if (num_word_tokens == 0) return
      val art = lookup_article(title)
      if (art == null) {
        warning("Skipping article %s, not in table", title)
        num_articles_with_word_counts_but_not_in_table += 1
        return
      }
      if (debug("wordcountarts"))
        writer.output_row(art)
      num_word_count_articles_by_split(art.split) += 1
      // If we are evaluating on the dev set, skip the test set and vice
      // versa, to save memory and avoid contaminating the results.
      if (art.split != "training" && art.split != Args.eval_set)
        return
      // Don't train on test set
      art.dist = WordDist(keys_dynarr.array, values_dynarr.array,
        keys_dynarr.length, note_globally = (art.split == "training"))
    }

    val task = new MeteredTask("article", "reading distributions of")
    errprint("Reading word counts from %s...", filename)
    errprint("")

    // Written this way because there's another line after the for loop,
    // corresponding to the else clause of the Python for loop
    breakable {
      for (line <- openr(filename)) {
        if (line.startsWith("Article title: ")) {
          if (title != null)
            one_article_probs()
          // Stop if we've reached the maximum
          if (task.item_processed(maxtime = Args.max_time_per_stage))
            break
          if ((Args.num_training_docs > 0 &&
            task.num_processed >= Args.num_training_docs)) {
            errprint("")
            errprint("Stopping because limit of %s documents reached",
              Args.num_training_docs)
            break
          }

          // Extract title and set it
          val titlere = "Article title: (.*)$".r
          line match {
            case titlere(ti) => title = ti
            case _ => assert(false)
          }
          keys_dynarr.clear()
          values_dynarr.clear()
          num_word_tokens = 0
        } else if (line.startsWith("Article coordinates) ") ||
          line.startsWith("Article ID: "))
          ()
        else {
          val linere = "(.*) = ([0-9]+)$".r
          line match {
            case linere(xword, xcount) => {
              var word = xword
              if (!Args.preserve_case_words) word = word.toLowerCase
              val count = xcount.toInt
              if (!(stopwords contains word) ||
                Args.include_stopwords_in_article_dists) {
                num_word_tokens += count
                keys_dynarr += memoize_word(word)
                values_dynarr += count
              }
            }
            case _ =>
              warning("Strange line, can't parse: title=%s: line=%s",
                title, line)
          }
        }
      }
      one_article_probs()
    }

    if (debug("wordcountarts"))
      stream.close()
    task.finish()
    num_articles_with_word_counts = task.num_processed
    output_resource_usage()
  }

  def finish_word_counts() {
    SmoothedWordDist.finish_global_distribution()
    finish_article_distributions()
    errprint("")
    errprint("-------------------------------------------------------------------------")
    errprint("Article count statistics:")
    var total_arts_in_table = 0
    var total_arts_with_word_counts = 0
    var total_arts_with_dists = 0
    for ((split, totaltoks) <- word_tokens_by_split) {
      errprint("For split '%s':", split)
      val arts_in_table = articles_by_split(split).length
      val arts_with_word_counts = num_word_count_articles_by_split(split)
      val arts_with_dists = num_dist_articles_by_split(split)
      total_arts_in_table += arts_in_table
      total_arts_with_word_counts += arts_with_word_counts
      total_arts_with_dists += arts_with_dists
      errprint("  %s articles in article table", arts_in_table)
      errprint("  %s articles with word counts seen (and in table)", arts_with_word_counts)
      errprint("  %s articles with distribution computed, %s total tokens, %.2f tokens/article",
        arts_with_dists, totaltoks,
        // Avoid division by zero
        totaltoks.toDouble / (arts_in_table + 1e-100))
    }
    errprint("Total: %s articles with word counts seen",
      num_articles_with_word_counts)
    errprint("Total: %s articles in article table", total_arts_in_table)
    errprint("Total: %s articles with word counts seen but not in article table",
      num_articles_with_word_counts_but_not_in_table)
    errprint("Total: %s articles with word counts seen (and in table)",
      total_arts_with_word_counts)
    errprint("Total: %s articles with distribution computed",
      total_arts_with_dists)
  }

  def construct_candidates(toponym: String) = {
    val lotop = toponym.toLowerCase
    lower_toponym_to_article(lotop)
  }

  def word_is_toponym(word: String) = {
    val lw = word.toLowerCase
    lower_toponym_to_article contains lw
  }
}

///////////////////////// Articles

/**
 * An "article" for geotagging.  Articles can come from Wikipedia, but
 * also from Twitter, etc., provided that the data is in the same format.
 * (In Twitter, generally each "article" is the set of tweets from a given
 * user.)
 */ 
class GeoArticle(params: Map[String, String]) extends Article(params)
  with EvaluationDocument {
  /**
   * Object containing word distribution of this article.
   */
  var dist: WordDist = null

  override def toString() = {
    var coordstr = if (coord != null) " at %s" format coord else ""
    val redirstr =
      if (redir.length > 0) ", redirect to %s" format redir else ""
    "%s(%s)%s%s" format (title, id, coordstr, redirstr)
  }

  // def __repr__() = "Article(%s)" format toString.encode("utf-8")

  def shortstr() = "%s" format title

  def struct() =
    <GeoArticle>
      <title>{ title }</title>
      <id>{ id }</id>
      {
        if (coord != null)
          <location>{ coord }</location>
      }
      {
        if (redir.length > 0)
          <redirectTo>{ redir }</redirectTo>
      }
    </GeoArticle>

  def distance_to_coord(coord2: Coord) = spheredist(coord, coord2)
}

/////////////////////////////////////////////////////////////////////////////
//                           Evaluation strategies                         //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for reading documents from a test file and doing
 * document geolocation on them (as opposed e.g. to toponym resolution).
 */
abstract class GeotagDocumentStrategy(val cellgrid: CellGrid) {
  /**
   * For a given word distribution (describing a test document), return
   * an Iterable of tuples, each listing a particular cell on the Earth
   * and a score of some sort.  The results should be in sorted order,
   * with better cells earlier.  Currently there is no guarantee about
   * the particular scores returned; for some strategies, lower scores
   * are better, while for others, higher scores are better.  Currently,
   * the wrapper code outputs the score but doesn't otherwise use it.
   */
  def return_ranked_cells(worddist: WordDist): Iterable[(GeoCell, Double)]
}

/**
 * Class that implements the baseline strategies for document geolocation.
 * 'baseline_strategy' specifies the particular strategy to use.
 */
class RandomGeotagDocumentStrategy(
  cellgrid: CellGrid
) extends GeotagDocumentStrategy(cellgrid) {
  def return_ranked_cells(worddist: WordDist) = {
    val cells = cellgrid.iter_nonempty_cells()
    val shuffled = (new Random()).shuffle(cells)
    (for (cell <- shuffled) yield (cell, 0.0))
  }
}

class MostPopularCellGeotagDocumentStrategy(
  cellgrid: CellGrid,
  internal_link: Boolean
) extends GeotagDocumentStrategy(cellgrid) {
  var cached_ranked_mps: Iterable[(GeoCell, Double)] = null
  def return_ranked_cells(worddist: WordDist) = {
    if (cached_ranked_mps == null) {
      cached_ranked_mps = (
        (for (cell <- cellgrid.iter_nonempty_cells())
          yield (cell,
            (if (internal_link)
               cell.worddist.incoming_links
             else
               cell.worddist.num_arts_for_links).toDouble)).
        toArray sortWith (_._2 > _._2))
    }
    cached_ranked_mps
  }
}

class CellDistMostCommonToponymGeotagDocumentStrategy(
  cellgrid: CellGrid
) extends GeotagDocumentStrategy(cellgrid) {
  val cdist_factory = new CellDistFactory(Args.lru_cache_size)

  def return_ranked_cells(worddist: WordDist) = {
    // Look for a toponym, then a proper noun, then any word.
    // FIXME: How can 'word' be null?
    // FIXME: Use invalid_word
    // FIXME: Should predicate be passed an index and have to do its own
    // unmemoizing?
    var maxword = worddist.find_most_common_word(
      word => word(0).isUpper && cellgrid.table.word_is_toponym(word))
    if (maxword == None) {
      maxword = worddist.find_most_common_word(
        word => word(0).isUpper)
    }
    if (maxword == None)
      maxword = worddist.find_most_common_word(word => true)
    cdist_factory.get_cell_dist(cellgrid, maxword.get).get_ranked_cells()
  }
}

class LinkMostCommonToponymGeotagDocumentStrategy(
  cellgrid: CellGrid
) extends GeotagDocumentStrategy(cellgrid) {
  def return_ranked_cells(worddist: WordDist) = {
    var maxword = worddist.find_most_common_word(
      word => word(0).isUpper && cellgrid.table.word_is_toponym(word))
    if (maxword == None) {
      maxword = worddist.find_most_common_word(
        word => cellgrid.table.word_is_toponym(word))
    }
    if (debug("commontop"))
      errprint("  maxword = %s", maxword)
    val cands =
      if (maxword != None)
        cellgrid.table.construct_candidates(
          unmemoize_word(maxword.get))
      else Seq[GeoArticle]()
    if (debug("commontop"))
      errprint("  candidates = %s", cands)
    // Sort candidate list by number of incoming links
    val candlinks =
      (for (cand <- cands) yield (cand, cand.adjusted_incoming_links.toDouble)).
        // sort by second element of tuple, in reverse order
        sortWith(_._2 > _._2)
    if (debug("commontop"))
      errprint("  sorted candidates = %s", candlinks)

    def find_good_cells_for_coord(cands: Iterable[(GeoArticle, Double)]) = {
      for {
        (cand, links) <- candlinks
        val cell = {
          val retval = cellgrid.find_best_cell_for_coord(cand.coord)
          if (retval == null)
            errprint("Strange, found no cell for candidate %s", cand)
          retval
        }
        if (cell != null)
      } yield (cell, links)
    }

    // Convert to cells
    val candcells = find_good_cells_for_coord(candlinks)

    if (debug("commontop"))
      errprint("  cell candidates = %s", candcells)

    // Append random cells and remove duplicates
    merge_numbered_sequences_uniquely(candcells,
      new RandomGeotagDocumentStrategy(cellgrid).return_ranked_cells(worddist))
  }
}

/**
 * Abstract class that implements a strategy for document geolocation that
 * involves directly comparing the article distribution against each cell
 * in turn and computing a score.
 *
 * @param prefer_minimum If true, lower scores are better; if false, higher
 *   scores are better.
 */
abstract class MinMaxScoreStrategy(
  cellgrid: CellGrid,
  prefer_minimum: Boolean
) extends GeotagDocumentStrategy(cellgrid) {
  /**
   * Function to return the score of an article distribution against a
   * cell.
   */
  def score_cell(worddist: WordDist, cell: GeoCell): Double

  /**
   * Compare a word distribution (for an article, typically) against all
   * cells. Return a sequence of tuples (cell, score) where 'cell'
   * indicates the cell and 'score' the score.
   */
  def return_ranked_cells(worddist: WordDist) = {
    val cell_buf = mutable.Buffer[(GeoCell, Double)]()
    for (
      cell <- cellgrid.iter_nonempty_cells(nonempty_word_dist = true)
    ) {
      if (debug("lots")) {
        errprint("Nonempty cell at indices %s = location %s, num_articles = %s",
          cell.describe_indices(), cell.describe_location(),
          cell.worddist.num_arts_for_word_dist)
      }

      val score = score_cell(worddist, cell)
      cell_buf += ((cell, score))
    }

    /* SCALABUG:
       If written simply as 'cell_buf sortWith (_._2 < _._2)',
       return type is mutable.Buffer.  However, if written as an
       if/then as follows, return type is Iterable, even though both
       forks have the same type of mutable.buffer!
     */
    if (prefer_minimum)
      cell_buf sortWith (_._2 < _._2)
    else
      cell_buf sortWith (_._2 > _._2)
  }
}

/**
 * Class that implements a strategy for document geolocation by computing
 * the KL-divergence between article and cell (approximately, how much
 * the word distributions differ).  Note that the KL-divergence as currently
 * implemented uses the smoothed word distributions.
 *
 * @param partial If true (the default), only do "partial" KL-divergence.
 * This only computes the divergence involving words in the article
 * distribution, rather than considering all words in the vocabulary.
 * @param symmetric If true, do a symmetric KL-divergence by computing
 * the divergence in both directions and averaging the two values.
 * (Not by default; the comparison is fundamentally asymmetric in
 * any case since it's comparing articles against cells.)
 */
class KLDivergenceStrategy(
  cellgrid: CellGrid,
  partial: Boolean = true,
  symmetric: Boolean = false
) extends MinMaxScoreStrategy(cellgrid, true) {

  def score_cell(worddist: WordDist, cell: GeoCell) = {
    var kldiv = worddist.fast_kl_divergence(cell.worddist,
      partial = partial)
    //var kldiv = worddist.test_kl_divergence(cell.worddist,
    //  partial = partial)
    if (symmetric) {
      val kldiv2 = cell.worddist.fast_kl_divergence(worddist,
        partial = partial)
      kldiv = (kldiv + kldiv2) / 2.0
    }
    //kldiv = worddist.test_kl_divergence(cell.worddist,
    //                           partial=partial)
    //errprint("For cell %s, KL divergence %.3f", cell, kldiv)
    kldiv
  }

  override def return_ranked_cells(worddist: WordDist) = {
    val cells = super.return_ranked_cells(worddist)

    if (debug("kldiv")) {
      // Print out the words that contribute most to the KL divergence, for
      // the top-ranked cells
      val num_contrib_cells = 5
      val num_contrib_words = 25
      errprint("")
      errprint("KL-divergence debugging info:")
      for (((cell, _), i) <- cells.take(num_contrib_cells) zipWithIndex) {
        val (_, contribs) =
          worddist.slow_kl_divergence_debug(
            cell.worddist, partial = partial,
            return_contributing_words = true)
        errprint("  At rank #%s, cell %s:", i + 1, cell)
        errprint("    %30s  %s", "Word", "KL-div contribution")
        errprint("    %s", "-" * 50)
        // sort by absolute value of second element of tuple, in reverse order
        val items = (contribs.toArray sortWith ((x, y) => abs(x._2) > abs(y._2))).
          take(num_contrib_words)
        for ((word, contribval) <- items)
          errprint("    %30s  %s", word, contribval)
        errprint("")
      }
    }

    cells
  }
}

/**
 * Class that implements a strategy for document geolocation by computing
 * the cosine similarity between the distributions of article and cell.
 * FIXME: We really should transform the distributions by TF/IDF before
 * doing this.
 *
 * @param smoothed If true, use the smoothed word distributions. (By default,
 * use unsmoothed distributions.)
 * @param partial If true, only do "partial" cosine similarity.
 * This only computes the similarity involving words in the article
 * distribution, rather than considering all words in the vocabulary.
 */
class CosineSimilarityStrategy(
  cellgrid: CellGrid,
  smoothed: Boolean = false,
  partial: Boolean = false
) extends MinMaxScoreStrategy(cellgrid, true) {

  def score_cell(worddist: WordDist, cell: GeoCell) = {
    var cossim =
      if (smoothed)
        worddist.fast_smoothed_cosine_similarity(cell.worddist,
          partial = partial)
      else
        worddist.fast_cosine_similarity(cell.worddist,
          partial = partial)
    assert(cossim >= 0.0)
    // Just in case of round-off problems
    assert(cossim <= 1.002)
    cossim = 1.002 - cossim
    cossim
  }
}

/** Use a Naive Bayes strategy for comparing document and cell. */
class NaiveBayesDocumentStrategy(
  cellgrid: CellGrid,
  use_baseline: Boolean = true
) extends MinMaxScoreStrategy(cellgrid, false) {

  def score_cell(worddist: WordDist, cell: GeoCell) = {
    // Determine respective weightings
    val (word_weight, baseline_weight) = (
      if (use_baseline) {
        if (Args.naive_bayes_weighting == "equal") (1.0, 1.0)
        else {
          val bw = Args.naive_bayes_baseline_weight.toDouble
          ((1.0 - bw) / worddist.num_word_tokens, bw)
        }
      } else (1.0, 0.0))

    val word_logprob = cell.worddist.get_nbayes_logprob(worddist)
    val baseline_logprob = log(cell.worddist.num_arts_for_links.toDouble /
      cellgrid.total_num_arts_for_links)
    val logprob = (word_weight * word_logprob +
      baseline_weight * baseline_logprob)
    logprob
  }
}

class AverageCellProbabilityStrategy(
  cellgrid: CellGrid
) extends GeotagDocumentStrategy(cellgrid) {
  val cdist_factory = new CellDistFactory(Args.lru_cache_size)

  def return_ranked_cells(worddist: WordDist) = {
    val celldist =
      cdist_factory.get_cell_dist_for_word_dist(cellgrid, worddist)
    celldist.get_ranked_cells()
  }
}

/////////////////////////////////////////////////////////////////////////////
//                                Segmentation                             //
/////////////////////////////////////////////////////////////////////////////

// General idea: Keep track of best possible segmentations up to a maximum
// number of segments.  Either do it using a maximum number of segmentations
// (e.g. 100 or 1000) or all within a given factor of the best score (the
// "beam width", e.g. 10^-4).  Then given the existing best segmentations,
// we search for new segmentations with more segments by looking at all
// possible ways of segmenting each of the existing best segments, and
// finding the best score for each of these.  This is a slow process -- for
// each segmentation, we have to iterate over all segments, and for each
// segment we have to look at all possible ways of splitting it, and for
// each split we have to look at all assignments of cells to the two
// new segments.  It also seems that we're likely to consider the same
// segmentation multiple times.
//
// In the case of per-word cell dists, we can maybe speed things up by
// computing the non-normalized distributions over each paragraph and then
// summing them up as necessary.

/////////////////////////////////////////////////////////////////////////////
//                                   Stopwords                             //
/////////////////////////////////////////////////////////////////////////////

object Stopwords {
  val stopwords_file_in_tg = "data/lists/stopwords.english"

  // Read in the list of stopwords from the given filename.
  def read_stopwords(stopwords_filename: String) = {
    def compute_stopwords_filename(filename: String) = {
      if (filename != null) filename
      else {
        val tgdir = TextGrounderInfo.textgrounder_dir
        // Concatenate directory and rest in most robust way
        new File(tgdir, stopwords_file_in_tg).toString
      }
    }
    val filename = compute_stopwords_filename(stopwords_filename)
    errprint("Reading stopwords from %s...", filename)
    openr(filename).toSet
  }
}

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

/**
 * Class retrieving command-line arguments or storing programmatic
 * configuration parameters.
 *
 * @param parser If specified, should be a parser for retrieving the
 *   value of command-line arguments from the command line.  Provided
 *   that the parser has been created and initialized by creating a
 *   previous instance of this same class with the same parser (a
 *   "shadow field" class), the variables below will be initialized with
 *   the values given by the user on the command line.  Otherwise, they
 *   will be initialized with the default values for the parameters.
 *   Because they are vars, they can be freely set to other values.
 *
 */
class GeolocateParameters(parser: ArgParser = null) {
  protected val ap =
    if (parser == null) new ArgParser("unknown") else parser

  //// Basic options for determining operating mode and strategy
  var mode =
    ap.option[String]("m", "mode",
      default = "geotag-documents",
      choices = Seq("geotag-toponyms",
        "geotag-documents",
        "generate-kml",
        "segment-geotag-documents"),
      help = """Action to perform.

'geotag-documents' finds the proper location for each document (or article)
in the test set.

'geotag-toponyms' finds the proper location for each toponym in the test set.
The test set is specified by --eval-file.  Default '%default'.

'segment-geotag-documents' simultaneously segments a document into sections
covering a specific location and determines that location. (Not yet
implemented.)

'generate-kml' generates KML files for some set of words, showing the
distribution over cells that the word determines.  Use '--kml-words' to
specify the words whose distributions should be outputted.  See also
'--kml-prefix' to specify the prefix of the files outputted, and
'--kml-transform' to specify the function to use (if any) to transform
the probabilities to make the distinctions among them more visible.
""")

  //// Input files
  var stopwords_file =
    ap.option[String]("stopwords-file",
      metavar = "FILE",
      help = """File containing list of stopwords.  If not specified,
a default list of English stopwords (stored in the TextGrounder distribution)
is used.""")

  var article_data_file =
    ap.multiOption[String]("a", "article-data-file",
      metavar = "FILE",
      help = """File containing info about Wikipedia or Twitter articles.
(For Twitter, an "article" is typically the set of all tweets from a single
user, and the name of the article is the user's name or some per-user
handle.) This file lists per-article information such as the article's title,
the split (training, dev, or test) that the article is in, and the article's
location.  It does not list the actual word-count information for the
articles; that is held in a separate counts file, specified using
--counts-file.

Multiple such files can be given by specifying the option multiple
times.""")
  var counts_file =
    ap.multiOption[String]("counts-file", "cf",
      metavar = "FILE",
      help = """File containing word counts for Wikipedia or Twitter articles.
There are scripts in the 'python' directory for generating counts in the
proper format.  Multiple such files can be given by specifying the
option multiple times.""")
  var eval_file =
    ap.multiOption[String]("e", "eval-file",
      metavar = "FILE",
      help = """File or directory containing files to evaluate on.
Multiple such files/directories can be given by specifying the option multiple
times.  If a directory is given, all files in the directory will be
considered (but if an error occurs upon parsing a file, it will be ignored).
Each file is read in and then disambiguation is performed.  Not used when
--eval-format=internal (which is the default with --mode=geotag-documents).""")

  //// Options indicating which documents to train on or evaluate
  var eval_set =
    ap.option[String]("eval-set", "es",
      default = "dev",
      choices = Seq("dev", "test"),
      canonicalize = Map("dev" -> Seq("devel")),
      help = """Set to use for evaluation when --eval-format=internal
and --mode=geotag-documents ('dev' or 'devel' for the development set,
'test' for the test set).  Default '%default'.""")
  var num_training_docs =
    ap.option[Int]("num-training-docs", "ntrain", default = 0,
      help = """Maximum number of training documents to use.
0 means no limit.  Default 0, i.e. no limit.""")
  var num_test_docs =
    ap.option[Int]("num-test-docs", "ntest", default = 0,
      help = """Maximum number of test (evaluation) documents to process.
0 means no limit.  Default 0, i.e. no limit.""")
  var skip_initial_test_docs =
    ap.option[Int]("skip-initial-test-docs", "skip-initial", default = 0,
      help = """Skip this many test docs at beginning.  Default 0, i.e.
don't skip any documents.""")
  var every_nth_test_doc =
    ap.option[Int]("every-nth-test-doc", "every-nth", default = 1,
      help = """Only process every Nth test doc.  Default 1, i.e.
process all.""")
  //  def skip_every_n_test_docs =
  //    ap.option[Int]("skip-every-n-test-docs", "skip-n", default=0,
  //      help="""Skip this many after each one processed.  Default 0.""")

  //// Options indicating how to generate the cells we compare against
  var degrees_per_cell =
    ap.option[Double]("degrees-per-cell", "dpc",
      default = 1.0,
      help = """Size (in degrees, a floating-point number) of the tiling
cells that cover the Earth.  Default %default. """)
  var miles_per_cell =
    ap.option[Double]("miles-per-cell", "mpc",
      help = """Size (in miles, a floating-point number) of the tiling
cells that cover the Earth.  If given, it overrides the value of
--degrees-per-cell.  No default, as the default of --degrees-per-cell
is used.""")
  var km_per_cell =
    ap.option[Double]("km-per-cell", "kpc",
      help = """Size (in kilometers, a floating-point number) of the tiling
cells that cover the Earth.  If given, it overrides the value of
--degrees-per-cell.  No default, as the default of --degrees-per-cell
is used.""")
  var width_of_multi_cell =
    ap.option[Int]("width-of-multi-cell", default = 1,
      help = """Width of the cell used to compute a statistical
distribution for geotagging purposes, in terms of number of tiling cells.
NOTE: It's unlikely you want to change this.  It may be removed entirely in
later versions.  In normal circumstances, the value is 1, i.e. use a single
tiling cell to compute each multi cell.  If the value is more than
1, the multi cells overlap.""")

  //// Options used when creating word distributions
  var preserve_case_words =
    ap.flag("preserve-case-words", "pcw",
      help = """Don't fold the case of words used to compute and
match against article distributions.  Note that in toponym resolution
(--mode=geotag-toponyms), this applies only to words in articles
(currently used only in Naive Bayes matching), not to toponyms, which
are always matched case-insensitively.""")
  var include_stopwords_in_article_dists =
    ap.flag("include-stopwords-in-article-dists",
      help = """Include stopwords when computing word distributions.""")
  var minimum_word_count =
    ap.option[Int]("minimum-word-count", "mwc",
      default = 1,
      help = """Minimum count of words to consider in word
distributions.  Words whose count is less than this value are ignored.""")

  //// Options used when doing Naive Bayes geotagging
  var naive_bayes_weighting =
    ap.option[String]("naive-bayes-weighting", "nbw",
      default = "equal",
      choices = Seq("equal", "equal-words", "distance-weighted"),
      help = """Strategy for weighting the different probabilities
that go into Naive Bayes.  If 'equal', do pure Naive Bayes, weighting the
prior probability (baseline) and all word probabilities the same.  If
'equal-words', weight all the words the same but collectively weight all words
against the baseline, giving the baseline weight according to --baseline-weight
and assigning the remainder to the words.  If 'distance-weighted', similar to
'equal-words' but don't weight each word the same as each other word; instead,
weight the words according to distance from the toponym.""")
  var naive_bayes_baseline_weight =
    ap.option[Double]("naive-bayes-baseline-weight", "nbbw",
      metavar = "WEIGHT",
      default = 0.5,
      help = """Relative weight to assign to the baseline (prior
probability) when doing weighted Naive Bayes.  Default %default.""")

  //// Options used when doing ACP geotagging
  var lru_cache_size =
    ap.option[Int]("lru-cache-size", "lru", default = 400,
      help = """Number of entries in the LRU cache.  Default %default.
Used only when --strategy=average-cell-probability.""")

  //// Debugging/output options
  var max_time_per_stage =
    ap.option[Double]("max-time-per-stage", "mts", default = 0.0,
      help = """Maximum time per stage in seconds.  If 0, no limit.
Used for testing purposes.  Default 0, i.e. no limit.""")
  var no_individual_results =
    ap.flag("no-individual-results", "no-results",
      help = """Don't show individual results for each test document.""")
  var oracle_results =
    ap.flag("oracle-results",
      help = """Only compute oracle results (much faster).""")
  var debug =
    ap.option[String]("d", "debug", metavar = "FLAGS",
      help = """Output debug info of the given types.  Multiple debug
parameters can be specified, indicating different types of info to output.
Separate parameters by spaces, colons or semicolons.  Params can be boolean,
if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
list-valued; multiple values are specified by including the parameter
multiple times, or by separating values by a comma.

The best way to figure out the possible parameters is by reading the
source code. (Look for references to debug("foo") for boolean params,
debugval("foo") for valueful params, or debuglist("foo") for list-valued
params.) Some known debug flags:

gridrank: For the given test article number (starting at 1), output
a grid of the predicted rank for cells around the true cell.
Multiple articles can have the rank output, e.g. --debug 'gridrank=45,58'
(This will output info for articles 45 and 58.) This output can be
postprocessed to generate nice graphs; this is used e.g. in Wing's thesis.

gridranksize: Size of the grid, in numbers of articles on a side.
This is a single number, and the grid will be a square centered on the
true cell. (Default currently 11.)

kldiv: Print out words contributing most to KL divergence.

wordcountarts: Regenerate article-data file, filtering out articles not
seen in any counts file.

some, lots, tons: General info of various sorts. (Document me.)

cell: Print out info on each cell of the Earth as it's generated.  Also
triggers some additional info when --mode=geotag-toponyms. (Document me.)

commontop: Extra info for debugging
 --baseline-strategy=link-most-common-toponym.

pcl-travel: Extra info for debugging --eval-format=pcl-travel.
""")

}

object Debug {
  // Debug params.  Different params indicate different info to output.
  // Specified using --debug.  Multiple params are separated by spaces,
  // colons or semicolons.  Params can be boolean, if given alone, or
  // valueful, if given as PARAM=VALUE.  Certain params are list-valued;
  // multiple values are specified by including the parameter multiple
  // times, or by separating values by a comma.
  val debug = booleanmap[String]()
  val debugval = stringmap[String]()
  val debuglist = bufmap[String, String]()

  var list_debug_params = Set[String]()

  // Register a list-valued debug param.
  def register_list_debug_param(param: String) {
    list_debug_params += param
  }

  def parse_debug_spec(debugspec: String) {
    val params = """[:;\s]+""".r.split(debugspec)
    // Allow params with values, and allow lists of values to be given
    // by repeating the param
    for (f <- params) {
      if (f contains '=') {
        val Array(param, value) = f.split("=", 2)
        if (list_debug_params contains param) {
          val values = "[,]".split(value)
          debuglist(param) ++= values
        } else
          debugval(param) = value
      } else
        debug(f) = true
    }
  }
}

/**
 * Class for programmatic access to document/etc. geolocation.
 *
 * NOTE: Currently this is a singleton object, not a class, because there
 * can be only one geolocation instance in existence.  This is because
 * of various other singleton objects (i.e. static methods/fields) scattered
 * throughout the code.  If this is a problem, let me know and I will
 * endeavor to fix it.
 *
 * Basic operation:
 *
 * 1. Create an instance of GeolocateParameters and populate it with the
 * appropriate parameters.  Don't pass in any ArgParser instance, as
 * is the default; that way, the parameters will get initialized to their
 * default values, and you only have to change the ones you want to be
 * non-default.
 * 2. Call set_parameters(), passing in the instance you just created.
 * 3. Call run().  The return value contains some evaluation results.
 *
 * NOTE: Currently, the GeolocateParameters instance is recorded directly
 * inside of this singleton object, without copying, and some of the fields are
 * changed to more canonical values.  If this is a problem, let me know and
 * I'll fix it.
 *
 * All evaluation output is currently written to standard error.
 * (There are some scripts to parse the output.) Some info is also returned
 * by the run() function.  See below.
 */
abstract class GeolocateDriver {
  type ParamType <: GeolocateParameters
  type StrategyType
  // NOTE: When different grids are allowed, we may set this to null here
  // and initialize it later based on a command-line option or whatever.
  var cellgrid = null: CellGrid
  var degrees_per_cell = 0.0
  var params: ParamType = _
  var stopwords: Set[String] = _
  var article_table: GeoArticleTable = _

  protected var argerror = default_error_handler _

  /**
   * Output the values of some internal parameters.  Only needed
   * for debugging.
   */
  def output_ancillary_parameters() {}

  /**
   * Default error handler for signalling an argument error.
   */
  def default_error_handler(string: String) {
    throw new IllegalArgumentException(string)
  }

  /**
   * Change the error handler.  Return the old handler.
   */
  def set_error_handler(handler: String => Unit) = {
    /* FUCK ME TO HELL.  Want to make this either a function to override
       or a class parameter, but both get awkward because Java type erasure
       means there's no easy way for a generic class to create a new object
       of the generic type. */
    val old_handler = argerror
    argerror = handler
    old_handler
  }

  protected def argument_needed(arg: String, arg_english: String = null) {
    val marg_english =
      if (arg_english == null)
        arg.replace("-", " ")
      else
        arg_english
    argerror("Must specify %s using --%s" format
      (marg_english, arg.replace("_", "-")))
  }

  def need_seq(value: Seq[String], arg: String, arg_english: String = null) {
    if (value.length == 0)
      argument_needed(arg, arg_english)
  }

  def need(value: String, arg: String, arg_english: String = null) {
    if (value == null || value.length == 0)
      argument_needed(arg, arg_english)
  }

  /**
   * Set the options to those as given.  NOTE: Currently, some of the
   * fields in this structure will be changed (canonicalized).  See above.
   * If options are illegal, an error will be signaled.
   *
   * @param options Object holding options to set
   */
  def set_parameters(args: ParamType) {
    GeolocateDriver.Args = args
    this.params = args

    /** Canonicalize arguments **/

    canonicalize_args(args)

    /** Set other values and check remaining args **/

    if (args.debug != null)
      parse_debug_spec(args.debug)

    def check_common_args(args: ParamType) {
      if (args.miles_per_cell < 0)
        argerror("Miles per cell must be positive if specified")
      if (args.km_per_cell < 0)
        argerror("Kilometers per cell must be positive if specified")
      if (args.degrees_per_cell < 0)
        argerror("Degrees per cell must be positive if specified")
      if (args.miles_per_cell > 0 && args.km_per_cell > 0)
        argerror("Only one of --miles-per-cell and --km-per-cell can be given")
      degrees_per_cell =
        if (args.miles_per_cell > 0)
          args.miles_per_cell / miles_per_degree
        else if (args.km_per_cell > 0)
          args.km_per_cell / km_per_degree
        else
          args.degrees_per_cell
      if (args.width_of_multi_cell <= 0)
        argerror("Width of multi cell must be positive")

      need_seq(args.article_data_file, "article-data-file")
    }

    check_common_args(args)
    check_remaining_args(args)
  }

  def canonicalize_args(Args: ParamType)
  def check_remaining_args(Args: ParamType)

  protected def initialize_cellgrid(table: GeoArticleTable) {
    cellgrid = new MultiRegularCellGrid(degrees_per_cell,
      Args.width_of_multi_cell, table)
  }

  protected def read_articles(table: GeoArticleTable, stopwords: Set[String]) {
    for (fn <- Args.article_data_file)
      table.read_article_data(fn, cellgrid)

    // Read in the words-counts file
    if (Args.counts_file.length > 0) {
      for (fn <- Args.counts_file)
        table.read_word_counts(fn, stopwords)
      table.finish_word_counts()
    }
  }

  protected def read_data_for_geotag_documents() {
    article_table = new GeoArticleTable()
    initialize_cellgrid(article_table)
    read_articles(article_table, stopwords)
  }

  protected def process_strategies[T](
    strat_unflat: Seq[Seq[(String, T)]])(
      geneval: (String, T) => EvaluationOutputter) = {
    val strats = strat_unflat reduce (_ ++ _)
    for ((stratname, strategy) <- strats) yield {
      val evalobj = geneval(stratname, strategy)
      // For --eval-format=internal, there is no eval file.  To make the
      // evaluation loop work properly, we pretend like there's a single
      // eval file whose value is null.
      val iterfiles =
        if (Args.eval_file.length > 0) Args.eval_file
        else Seq[String](null)
      evalobj.evaluate_and_output_results(iterfiles)
      (stratname, strategy, evalobj)
    }
  }

  def run() = {
    stopwords = Stopwords.read_stopwords(Args.stopwords_file)
    implement_run(params)
  }

  /**
   * Actually do the work.
   */
  def implement_run(args: ParamType): Seq[(String, StrategyType, EvaluationOutputter)]
}

object GeolocateDriver {
  var Args: GeolocateParameters = _
}

class GenerateKMLParameters(
  parser: ArgParser = null
) extends GeolocateParameters(parser) {
  //// Options used only in KML generation (--mode=generate-kml)
  var kml_words =
    ap.option[String]("k", "kml-words", "kw",
      help = """Words to generate KML distributions for, when
--mode=generate-kml.  Each word should be separated by a comma.  A separate
file is generated for each word, using the value of '--kml-prefix' and adding
'.kml' to it.""")
  var kml_prefix =
    ap.option[String]("kml-prefix", "kp",
      default = "kml-dist.",
      help = """Prefix to use for KML files outputted in --mode=generate-kml.
The actual filename is created by appending the word, and then the suffix
'.kml'.  Default '%default'.""")
  var kml_transform =
    ap.option[String]("kml-transform", "kt", "kx",
      default = "none",
      choices = Seq("none", "log", "logsquared"),
      help = """Type of transformation to apply to the probabilities
when generating KML (--mode=generate-kml), possibly to try and make the
low values more visible.  Possibilities are 'none' (no transformation),
'log' (take the log), and 'logsquared' (negative of squared log).  Default
'%default'.""")
  var kml_max_height =
    ap.option[Double]("kml-max-height", "kmh",
      default = 2000000.0,
      help = """Height of highest bar, in meters.  Default %default.""")
}

class GenerateKMLDriver extends GeolocateDriver {
  type ParamType = GenerateKMLParameters
  type StrategyType = Nothing

  def canonicalize_args(args: ParamType) {
  }

  def check_remaining_args(args: ParamType) {
    need(args.kml_words, "kml-words")
  }

  /**
   * Do the actual KML generation.  Some tracking info written to stderr.
   * KML files created and written on disk.
   */

  def implement_run(args: ParamType) = {
    read_data_for_geotag_documents()
    cellgrid.finish()
    val cdist_factory = new CellDistFactory(args.lru_cache_size)
    val words = args.kml_words.split(',')
    for (word <- words) {
      val celldist = cdist_factory.get_cell_dist(cellgrid, memoize_word(word))
      if (!celldist.normalized) {
        warning("""Non-normalized distribution, apparently word %s not seen anywhere.
Not generating an empty KML file.""", word)
      } else {
        val kmlparams = new KMLParameters()
        kmlparams.kml_max_height = args.kml_max_height
        kmlparams.kml_transform = args.kml_transform
        celldist.generate_kml_file("%s%s.kml" format (args.kml_prefix, word),
          kmlparams)
      }
    }
    null
  }
}

class GeolocateDocumentParameters(
  parser: ArgParser = null
) extends GeolocateParameters(parser) {
  var eval_format =
    ap.option[String]("f", "eval-format",
      default = "internal",
      choices = Seq("internal", "raw-text", "pcl-travel"),
      help = """Format of evaluation file(s).  The evaluation files themselves
are specified using --eval-file.  The following formats are
recognized:

'internal' is the normal format.  It means to consider articles to be
documents to evaluate, and to use the development or test set specified
in the article-data file as the set of documents to evaluate.  There is
no eval file for this format.

'raw-text' assumes that the eval file is simply raw text.  (NOT YET
IMPLEMENTED.)

'pcl-travel' is another alternative.  It assumes that each evaluation file
is in PCL-Travel XML format, and uses each chapter in the evaluation
file as a document to evaluate.""")

  var strategy =
    ap.multiOption[String]("s", "strategy",
      //      choices=Seq(
      //        "baseline", "none",
      //        "full-kl-divergence",
      //        "partial-kl-divergence",
      //        "symmetric-full-kl-divergence",
      //        "symmetric-partial-kl-divergence",
      //        "cosine-similarity",
      //        "partial-cosine-similarity",
      //        "smoothed-cosine-similarity",
      //        "smoothed-partial-cosine-similarity",
      //        "average-cell-probability",
      //        "naive-bayes-with-baseline",
      //        "naive-bayes-no-baseline",
      //        ),
      canonicalize = Map(
        "baseline" -> null, "none" -> null,
        "full-kl-divergence" ->
          Seq("full-kldiv", "full-kl"),
        "partial-kl-divergence" ->
          Seq("partial-kldiv", "partial-kl", "part-kl"),
        "symmetric-full-kl-divergence" ->
          Seq("symmetric-full-kldiv", "symmetric-full-kl", "sym-full-kl"),
        "symmetric-partial-kl-divergence" ->
          Seq("symmetric-partial-kldiv", "symmetric-partial-kl", "sym-part-kl"),
        "cosine-similarity" ->
          Seq("cossim"),
        "partial-cosine-similarity" ->
          Seq("partial-cossim", "part-cossim"),
        "smoothed-cosine-similarity" ->
          Seq("smoothed-cossim"),
        "smoothed-partial-cosine-similarity" ->
          Seq("smoothed-partial-cossim", "smoothed-part-cossim"),
        "average-cell-probability" ->
          Seq("avg-cell-prob", "acp"),
        "naive-bayes-with-baseline" ->
          Seq("nb-base"),
        "naive-bayes-no-baseline" ->
          Seq("nb-nobase")),
      help = """Strategy/strategies to use for geotagging.
'baseline' means just use the baseline strategy (see --baseline-strategy).

'none' means don't do any geotagging.  Useful for testing the parts that
read in data and generate internal structures.

The other possible values depend on which mode is in use
(--mode=geotag-toponyms or --mode=geotag-documents).

For geotag-toponyms:

'naive-bayes-with-baseline' (or 'nb-base') means also use the words around the
toponym to be disambiguated, in a Naive-Bayes scheme, using the baseline as the
prior probability; 'naive-bayes-no-baseline' (or 'nb-nobase') means use uniform
prior probability.  Default is 'baseline'.

For geotag-documents:

'full-kl-divergence' (or 'full-kldiv') searches for the cell where the KL
divergence between the article and cell is smallest.
'partial-kl-divergence' (or 'partial-kldiv') is similar but uses an
abbreviated KL divergence measure that only considers the words seen in the
article; empirically, this appears to work just as well as the full KL
divergence. 'average-cell-probability' (or
'celldist') involves computing, for each word, a probability distribution over
cells using the word distribution of each cell, and then combining the
distributions over all words in an article, weighted by the count the word in
the article.  Default is 'partial-kl-divergence'.

NOTE: Multiple --strategy options can be given, and each strategy will
be tried, one after the other.""")

  var baseline_strategy =
    ap.multiOption[String]("baseline-strategy", "bs",
      choices = Seq("internal-link", "random",
        "num-articles", "link-most-common-toponym",
        "cell-distribution-most-common-toponym"),
      canonicalize = Map(
        "internal-link" -> Seq("link"),
        "num-articles" -> Seq("num-arts", "numarts"),
        "cell-distribution-most-common-toponym" ->
          Seq("celldist-most-common-toponym")),
      help = """Strategy to use to compute the baseline.

'internal-link' (or 'link') means use number of internal links pointing to the
article or cell.

'random' means choose randomly.

'num-articles' (or 'num-arts' or 'numarts'; only in cell-type matching) means
use number of articles in cell.

'link-most-common-toponym' (only in --mode=geotag-documents) means to look
for the toponym that occurs the most number of times in the article, and
then use the internal-link baseline to match it to a location.

'celldist-most-common-toponym' (only in --mode=geotag-documents) is similar,
but uses the cell distribution of the most common toponym.

Default '%default'.

NOTE: Multiple --baseline-strategy options can be given, and each strategy will
be tried, one after the other.  Currently, however, the *-most-common-toponym
strategies cannot be mixed with other baseline strategies, or with non-baseline
strategies, since they require that --preserve-case-words be set internally.""")
}

class GeolocateDocumentDriver extends GeolocateDriver {
  type ParamType = GeolocateDocumentParameters
  type StrategyType = GeotagDocumentStrategy

  def canonicalize_args(args: ParamType) {
    if (args.strategy.length == 0)
      args.strategy = Seq("partial-kl-divergence")

    if (args.baseline_strategy.length == 0)
      args.baseline_strategy = Seq("internal-link")

    if (args.strategy contains "baseline") {
      var need_case = false
      var need_no_case = false
      for (bstrat <- args.baseline_strategy) {
        if (bstrat.endsWith("most-common-toponym"))
          need_case = true
        else
          need_no_case = true
      }
      if (need_case) {
        if (args.strategy.length > 1 || need_no_case) {
          // That's because we have to set --preserve-case-words, which we
          // generally don't want set for other strategies and which affects
          // the way we construct the training-document distributions.
          argerror("Can't currently mix *-most-common-toponym baseline strategy with other strategies")
        }
        args.preserve_case_words = true
      }
    }
  }

  def check_remaining_args(args: ParamType) {
    if (args.counts_file.length == 0)
      argerror("Must specify counts file")

    if (args.eval_format == "raw-text") {
      // FIXME!!!!
      argerror("Raw-text reading not implemented yet")
    }

    if (args.eval_format == "internal") {
      if (args.eval_file.length > 0)
        argerror("--eval-file should not be given when --eval-format=internal")
    } else
      need_seq(args.eval_file, "eval-file", "evaluation file(s)")
  }

  /**
   * Do the actual document geolocation.  Results to stderr (see above), and
   * also returned.
   *
   * The current return type is as follows:
   *
   * Seq[(java.lang.String, GeotagDocumentStrategy, scala.collection.mutable.Map[evalobj.Document,opennlp.textgrounder.geolocate.EvaluationResult])] where val evalobj: opennlp.textgrounder.geolocate.TestFileEvaluator
   *
   * This means you get a sequence of tuples of
   * (strategyname, strategy, results)
   * where:
   * strategyname = name of strategy as given on command line
   * strategy = strategy object
   * results = map listing results for each document (an abstract type
   * defined in TestFileEvaluator; the result type EvaluationResult
   * is practically an abstract type, too -- the most useful dynamic
   * type in practice is ArticleEvaluationResult)
   */

  def implement_run(args: ParamType) = {
    read_data_for_geotag_documents()
    cellgrid.finish()

    val strats = (
      for (stratname <- args.strategy) yield {
        if (stratname == "baseline") {
          for (basestratname <- args.baseline_strategy) yield {
            val strategy = basestratname match {
              case "link-most-common-toponym" =>
                new LinkMostCommonToponymGeotagDocumentStrategy(cellgrid)
              case "celldist-most-common-toponym" =>
                new CellDistMostCommonToponymGeotagDocumentStrategy(cellgrid)
              case "random" =>
                new RandomGeotagDocumentStrategy(cellgrid)
              case "internal-link" =>
                new MostPopularCellGeotagDocumentStrategy(cellgrid, true)
              case "num-articles" =>
                new MostPopularCellGeotagDocumentStrategy(cellgrid, false)
              case _ => {
                assert(false,
                  "Internal error: Unhandled strategy " + basestratname);
                null
              }
            }
            ("baseline " + basestratname, strategy)
          }
        } else {
          val strategy =
            if (stratname.startsWith("naive-bayes-"))
              new NaiveBayesDocumentStrategy(cellgrid,
                use_baseline = (stratname == "naive-bayes-with-baseline"))
            else stratname match {
              case "average-cell-probability" =>
                new AverageCellProbabilityStrategy(cellgrid)
              case "cosine-similarity" =>
                new CosineSimilarityStrategy(cellgrid, smoothed = false, partial = false)
              case "partial-cosine-similarity" =>
                new CosineSimilarityStrategy(cellgrid, smoothed = false, partial = true)
              case "smoothed-cosine-similarity" =>
                new CosineSimilarityStrategy(cellgrid, smoothed = true, partial = false)
              case "smoothed-partial-cosine-similarity" =>
                new CosineSimilarityStrategy(cellgrid, smoothed = true, partial = true)
              case "full-kl-divergence" =>
                new KLDivergenceStrategy(cellgrid, symmetric = false, partial = false)
              case "partial-kl-divergence" =>
                new KLDivergenceStrategy(cellgrid, symmetric = false, partial = true)
              case "symmetric-full-kl-divergence" =>
                new KLDivergenceStrategy(cellgrid, symmetric = true, partial = false)
              case "symmetric-partial-kl-divergence" =>
                new KLDivergenceStrategy(cellgrid, symmetric = true, partial = true)
              case "none" =>
                null
            }
          if (strategy != null)
            Seq((stratname, strategy))
          else
            Seq()
        }
      })
    process_strategies(strats)((stratname, strategy) => {
      val evaluator =
        // Generate reader object
        if (args.eval_format == "pcl-travel")
          new PCLTravelGeotagDocumentEvaluator(strategy, stratname, this)
        else
          new ArticleGeotagDocumentEvaluator(strategy, stratname, this)
      new DefaultEvaluationOutputter(stratname, evaluator)
    })
  }
}

abstract class GeolocateApp(appname: String) extends ExperimentApp(appname) {
  type ParamType <: GeolocateParameters
  type ArgType = ParamType
  type DriverType <: GeolocateDriver
  val driver = create_driver()

  def create_driver(): DriverType

  def argerror(str: String) {
    the_argparser.error(str)
  }

  override def output_ancillary_parameters() {
    driver.output_ancillary_parameters()
  }

  def handle_arguments(args: Seq[String]) {
    driver.set_error_handler(argerror _)
    driver.set_parameters(argholder.asInstanceOf[driver.ParamType])
  }

  def implement_main(args: Seq[String]) {
    driver.run()
  }
}

object GeolocateDocumentApp extends GeolocateApp("geolocate-documents") {
  type ParamType = GeolocateDocumentParameters
  type DriverType = GeolocateDocumentDriver
  // FUCKING TYPE ERASURE
  def create_arg_class() = new ParamType(the_argparser)
  def create_driver() = new DriverType()
  main()
}

object GenerateKMLApp extends GeolocateApp("generate-kml") {
  type ParamType = GenerateKMLParameters
  type DriverType = GenerateKMLDriver
  // FUCKING TYPE ERASURE
  def create_arg_class() = new ParamType(the_argparser)
  def create_driver() = new DriverType()
  main()
}

