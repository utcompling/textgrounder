///////////////////////////////////////////////////////////////////////////////
//  GridLocate.scala
//
//  Copyright (C) 2010-2014 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package gridlocate

import collection.mutable

import util.argparser._
import util.collection.is_sorted
import util.debug._
import util.error._
import util.experiment._
import util.io.{FileHandler,localfh}
import util.metering._
import util.os.output_resource_usage
import util.print._
import util.spherical._
import util.textdb._
import util.verbose._

import learning._
import learning.perceptron._
import learning.mlogit._
import learning.tadm._
import learning.vowpalwabbit._
import langmodel._

/*

This file contains the main driver module for GridLocate projects.
"GridLocate" means applications that involve searching for the best value
(in some space) for a given test document by dividing the space into a
grid of some sort (not necessarily regular, and not necessarily even
with non-overlapping grid cells), aggregating all the documents in a
given cell, and finding the best value by searching for the best grid
cell and then returning some representative point (e.g.  the center) as
the best value.  The original application was for geolocation, i.e.
assigning a latitude/longitude coordinate to a document, and the grid
was a regular tiling of the Earth's surface based on "squares" of a given
amount of latitude and longitude on each side.  But other applications
are possible, e.g. locating the date of a given biographical document,
where the space ranges over dates in time (one-dimensional) rather than
over the Earth's surface (two-dimensional).

*/

/**
 * Constants used in various places esp. debugging code.
 */
object GridLocateConstants {
  val kldiv_num_contrib_cells = 5
  val kldiv_num_contrib_words = 25
  val relcontribgrams_to_print = 15

  // For outputting periodic result status when evaluating a test set
  // of documents
  val time_between_status = 300
  val docs_between_status = 10

  // For computing statistics about predictions during evaluation
  val top_n_for_oracle_dists =
    Seq(1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100)
  val max_rank_for_exact_incorrect = 10

  // Number of entries in the LRU cache.  Used only when
  // --ranker=average-cell-probability.
  val default_lru_cache_size = 400
  def lru_cache_size = debugint("lru-cache-size", default_lru_cache_size)
}

/**
 * Mixin holding basic GridLocate parameters.
 */
trait GridLocateBasicParameters {
  this: GridLocateParameters =>

  var verbose =
    ap.flag("verbose", "v",
      help = """Output more information about what's going on.""")

  var language =
    ap.option[String]("language", "lang",
       default = "eng",
       metavar = "LANG",
       aliasedChoices = Seq(
         Seq("eng", "en"),
         Seq("por", "pt"),
         Seq("deu", "de")
       ),
       help = """Name of language of corpus.  Currently used only to
initialize the value of the stopwords file, if not explicitly set.
Two- and three-letter ISO-639 codes can be used.  Currently recognized:
English (en, eng); German (de, deu); Portuguese (pt, por).""")

  //// Input files

  var input =
    ap.multiOption[String]("input", "i", "input-corpus", "ic",
      help = """One or more training corpora. The corpora are in the format
of a textdb database (a type of flat-file database, with separate schema and
data files). A corpus can be specified in any of the following ways:
Either the data or schema file of the database; the common prefix of the
two; or the directory containing them, provided there is only one textdb
in the directory.

Training corpora can be specified either using '--input' (which may be
repeated multiple times to specify multiple corpora) or as positional
parameters after all options, or a combination of both.

A separate grid is created for each corpus, and test documents are evaluated
against all cells in all grids.  This allows, for example, a given corpus to
be clustered into sub-corpora, each listed as a separate corpus -- or even
for multiple such clusterings to be given.

Documents in the corpus can be Wikipedia articles, individual tweets
in Twitter, the set of all tweets for a given user, etc.  The corpus
generally contains one or more "views" on the raw data comprising
the corpus, with different views corresponding to differing ways
of representing the original text of the documents -- as raw,
word-split text; as unigram word counts; as n-gram word counts;
etc.  Each such view has a schema file and one or more document
files.  The latter contains all the data for describing each document,
including title, split (training, dev or test) and other metadata,
as well as the text or word counts that are used to create the
language model of the document.  The document files are laid out
in a very simple database format, consisting of one document per
line, where each line is composed of a fixed number of fields,
separated by TAB characters. (E.g. one field would list the title,
another the split, another all the word counts, etc.) A separate
schema file lists the name of each expected field.  Some of these
names (e.g. "title", "split", "text", "coord") have pre-defined
meanings, but arbitrary names are allowed, so that additional
corpus-specific information can be provided (e.g. retweet info for
tweets that were retweeted from some other tweet, article ID for a
Wikipedia article, etc.).""")

  var train =
    ap.multiPositional[String]("train",
      help = """One or more training corpora. See '--input'.""")

  if (ap.parsedValues) {
    if (input.size + train.size == 0)
      ap.error("Must specify a training corpus")
  }
}

trait GridLocateLangModelParameters {
  this: GridLocateParameters =>

  //// Options used when creating language models
  var jelinek_factor_default = 0.3
  var dirichlet_factor_default = 500.0
  val lang_model_choices = Seq(
        Seq("pseudo-good-turing", "pgt"),
        Seq("dirichlet"),
        Seq("jelinek-mercer", "jelinek"),
        Seq("unsmoothed-ngram"))
  var lang_model =
    ap.optionWithParams[String]("lang-model", "lm", "word-dist", "wd",
      default = ("dirichlet", ":1,000,000"),
      aliasedChoices = lang_model_choices,
      help = """Type of language model to use.  Possibilities are
'pseudo-good-turing' (a simplified version of Good-Turing smoothing over a
unigram language model), 'dirichlet' (Dirichlet smoothing over a unigram
language model), 'jelinek' or 'jelinek-mercer' (Jelinek-Mercer smoothing over
a unigram language model), and 'unsmoothed-ngram' (an unsmoothed n-gram
language model). Default '%%default'.

For Dirichlet and Jelinek, an optional smoothing parameter can be given,
following a colon, e.g. 'jelinek:0.2' or 'dirichlet:10000'. Commas and
underscores can be used to make large numbers easier to read. The higher
the value, the more smoothing is done. Default is %g for Jelinek and
%g for Dirichlet. The parameter must be between 0.0 and 1.0 for Jelinek,
and >= 0.0 for Dirichlet. In both cases, a value of 0.0 means no smoothing
(making both methods equivalent). See below for more explanation.

An unsmoothed language model is simply the maximum-likelihood (MLE)
distribution, which assigns probability to words according to how often
they have been observed in the document, with all words that do not occur
in the document assigned 0 probability. These zero-value probabilities
are problematic both conceptually (in that it means that words that happen
not to have been seen so far can never be seen in the future) and
practically, and so it is usually better to "smooth" an MLE distribution
to ensure that all words have a non-zero probability (even if small).

All of the implemented smoothed language models operate by discounting, i.e.
taking away a certain amount of probability mass from the words in the MLE
distribution and distributing it over the unseen words, in proportion to
their probability across all documents (i.e. their global distribution).
Discounting is done either by interpolation (taking a weighted average
of the MLE and global distributions, so that the global-distribution
statistics are mixed into all words) or back-off (using the global
distribution only for words not seen in the document). In both cases a
"discounting factor" (between 0.0 and 1.0) determines what fraction of
the total probability mass is reserved for the global distribution: A
value of 0.0 means no smoothing, while a value of 1.0 means that the
MLE distribution is totally ignored in favor of the global distribution.

In Jelinek smoothing, the discounting factor is directly specified by
the smoothing parameter.

In Dirichlet smoothing, the discounting factor is
m/(|D|+m) = 1/(1+|D|/m) where m is the smoothing parameter and |D|
is the length of the document in words. This means that the longer the
document, the smaller the discounting factor: i.e. large documents are
smoothed less than small ones because the MLE is expected to be more
accurate (because more information is available). Furthermore, the value
of m can be thought of as indicating (very roughly) the expected
document size: When the document length |D| = m, the discounting factor
is 1/2, and in general if |D| = n*m, then the discounting factor is
1/(1+n), so that for documents significantly larger than m, the MLE
distribution is weighted much more than the global distribution, while
for for documents significantly smaller than m, the reverse is true.

Note that the smoothing mostly affects the cell language models rather
than the test document language models; hence the value of m should
reflect this.

Pseudo-Good-Turing has no smoothing parameter, and automatically sets
the discounting factor equal to the fraction of MLE probability mass
occupied by words seen once. The intuition here is that the probability
of seeing an unseen word can be estimated by the fraction of tokens that
correspond to words seen only once.

By default, Jelinek and Dirichlet do interpolation and Pseudo-Good-Turing
does back-off, by this can be overridden using --interpolate.""" format (
  jelinek_factor_default, dirichlet_factor_default))
  var interpolate =
    ap.option[String]("interpolate",
      default = "default",
      aliasedChoices = Seq(
        Seq("yes", "interpolate"),
        Seq("no", "backoff"),
        Seq("default")),
      help = """Whether to do interpolation rather than back-off.
Possibilities are 'yes', 'no', and 'default' (which means 'yes' when doing
Dirichlet or Jelinek-Mercer smoothing, 'no' when doing pseudo-Good-Turing
smoothing).""")

  /**
   * Type of lang model used in the cell grid.
   */
  lazy val grid_lang_model_type = {
    if (lang_model == (("unsmoothed-ngram", ""))) "ngram"
    else "unigram"
  }

  var preserve_case_words =
    ap.flag("preserve-case-words", "pcw",
      help = """Don't fold the case of words used to compute and
match against document language models.  Note that in toponym resolution,
this applies only to words in documents (currently used only in Naive Bayes
matching), not to toponyms, which are always matched case-insensitively.""")
  var no_stopwords =
    ap.flag("no-stopwords",
      help = """Don't remove any stopwords from language models.""")
  var minimum_word_count =
    ap.option[Int]("minimum-word-count", "mwc", metavar = "NUM",
      default = 1,
      must = be_>(0),
      help = """Minimum count of words to consider in language models.
Words whose count is less than this value are ignored.""")
  var max_ngram =
    ap.option[Int]("max-ngram", "mn", metavar = "NUM",
      default = 0,
      must = be_>=(0),
      help = """Maximum length of n-grams to include in an n-gram language
model. Any larger n-grams included in the source (e.g. corpus)
will be ignored. A value of 0 means don't filter any n-grams.  See
also `--raw-text-max-ngram`, which controls the maximum length of n-grams
generated from a raw document.""")
  var raw_text_max_ngram =
    ap.option[Int]("raw-text-max-ngram", "rtmn", metavar = "NUM",
      default = 3,
      must = be_>=(0),
      help = """Maximum length of n-grams to generate when generating
n-grams from a raw document.  See also `--max-ngram`, which filters out
all n-grams above a particular length from an existing corpus of n-grams.
The `--max-ngram` filter apples to n-grams generated from raw documents,
so if `--max-gram` is set to a positive number, its value should be >=
the value of `--raw-text-max-ngram` or additional unnecessary work will
be done generating higher-length n-grams that will then just be thrown
away.""")
 var tf_idf =
   ap.flag("tf-idf", "tfidf",
      help = """Adjust word counts according to TF-IDF weighting (i.e.
downweight words that occur in many documents).""")

  var stopwords_file =
    ap.option[String]("stopwords-file",
      metavar = "FILE",
      help = """File containing list of stopwords.  These words will be
ignored when creating a language model. Format is one word per line.
If not specified, a default list of English stopwords (stored in the
TextGrounder distribution) is used.""")

  var whitelist_file =
    ap.option[String]("whitelist-file",
       metavar = "FILE",
       help = """File containing a whitelist of words. If specified, only
words in the list will be used when creating a language model from a corpus;
all others will be ignored. Format is one word per line. When specifying a
whitelist, the stopwords in '--stopwords-file' will still be ignored.""")

  var whitelist_weight_file =
    ap.option[String]("whitelist-weight-file",
       metavar = "FILE",
       help = """File containing a set of words and weights, with some
fraction of the top-weighted words serving as a whitelist. Unlike the file in
'--weights-file', the words are not weighted non-uniformly. To specify the
cutoff, use either '--weight-cutoff-value' or '--weight-cutoff-percent'.
The file should have the word and weight separated by a tab character.""")

  var word_weight_file =
    ap.option[String]("word-weight-file",
       metavar = "FILE",
       help = """File containing a set of word weights for weighting words
non-uniformly (e.g. so that more geographically informative words are weighted
higher). The file should have the word and weight separated by a tab
character. The weights do not need to be normalized but must not be negative.
They will be normalized so that the average is 1. Each time a word is read in,
it will be given a count corresponding to its normalized weight rather than a
count of 1.

The parameter value can be any of the following: Either the data or schema
file of the database; the common prefix of the two; or the directory containing
them, provided there is only one textdb in the directory.""")

  var weight_cutoff_value =
    ap.option[Double]("weight-cutoff-value", "wcv",
       metavar = "WEIGHT",
       default = Double.MinValue,
       help = """If given, ignore words whose weights are below the given
value. This is used by '--whilelist-weight-file' and '--word-weight-file'.
If you specify this in conjunction with '--word-weight-file', you might also
want to specify `--missing-word-weight 0` so that words with a missing or
ignored weight are assigned a weight of 0 rather than the average of the
weights that have been seen.""")

  var weight_cutoff_percent =
    ap.option[Double]("weight-cutoff-percent", "wcp",
       metavar = "WEIGHT",
       default = 100.0,
       must = be_and(be_>=(0.0), be_<=(100.0)),
       help = """If given, use only the top N percent of the weights, for the
value N specified.  This is used by '--whilelist-weight-file' and
'--word-weight-file'. If you specify this in conjunction with
'--word-weight-file', you might also want to specify `--missing-word-weight 0`
so that words with a missing or ignored weight are assigned a weight of 0
rather than the average of the weights that have been seen.""")

  var missing_word_weight =
    ap.option[Double]("missing-word-weight", "mww",
       metavar = "WEIGHT",
       default = -1.0,
       help = """Weight to use for words not given in the word-weight
file, when `--word-weight-file` is given. If negative (the default), use
the average of all weights in the file, which treats them as if they
have no special weight. It is possible to set this value to zero, in
which case unspecified words will be ignored.""")

  var weight_abs =
    ap.flag("weight-abs", "wa",
      help = """Take the absolute value of weights as we read them in.""")

  var output_training_cell_lang_models =
    ap.flag("output-training-cell-lang-models", "output-training-cells", "otc",
      help = """Output the training cell lang models after they've been trained.""")
}

trait GridLocateCellParameters {
  this: GridLocateParameters =>

  //// Options relating to cells
  var center_method =
    ap.option[String]("center-method", "cm", metavar = "CENTER_METHOD",
      default = "centroid",
      choices = Seq("centroid", "center"),
      help = """Chooses whether to use true center or centroid for cell
central-point calculation. Options are either 'centroid' or 'center'.
Default '%default'.""")

  var salience_file =
    ap.option[String]("salience-file",
       metavar = "FILE",
       help = """File containing a set of salient coordinates for identifying
grid cells, in textdb format. These coordinates can be e.g. cities from a
gazetteer with their population used as the salience value. The salience
values are considered to be additional cell-level salience values and will
be added to the total salience of a cell in addition to the salience of each
document (which is specified in the document file or files). The salience
values are used in the 'salience' method for computing the prior Naive Bayes
probability of a cell, in the 'salience' baseline ranker, and for diagnostic
purposes when printing out a cell, e.g. in '--print-results'. In the latter
case, the single-most salient item in the cell, whether document or salient
coordinate, is printed out; this makes it possible, e.g., to determine
where approximately a given cell is located by its largest city.

The value can be any of the following: Either the data or schema file of
the database; the common prefix of the two; or the directory containing
them, provided there is only one textdb in the directory.

The file should be in textdb format, with `name`, `coord` and `salience`
fields. If omitted, cells will be identified by the most salient document
in the cell, if documents have their own salience values.""")
}

//// Options indicating which documents to train on or evaluate
trait GridLocateEvalParameters {
  this: GridLocateParameters =>

  //// Eval input options
  var eval_set =
    ap.option[String]("eval-set", "es", metavar = "SET",
      default = "dev",
      // The first item of each must agree with the suffixes used, currently
      // 'training', 'dev', 'test'.
      aliasedChoices = Seq(Seq("training", "train"), Seq("dev", "devel"),
        Seq("test")),
      help = """Set of documents to evaluate ('train' or 'training' for the
training set, 'dev' or 'devel' for the development set, 'test' for the test
set). Default '%default'.""")

  var eval_file =
    ap.multiOption[String]("e", "eval-file", "ef",
      metavar = "FILE",
      help = """File or directory containing files to evaluate on.
Multiple such files/directories can be given by specifying the option multiple
times. If a directory is given, all files in the directory will be
considered (but if an error occurs upon parsing a file, it will be ignored).
Each file is read in and then geolocation is performed.

If --eval-format=textdb (the default), this option can be omitted, in which
case it defaults to the same locations as are specified for the training
corpus or corpora. This does not mean that evaluation will happen on the
training set, because generally a textdb corpus used for geolocation has
separate slices for training, devel and test (identified by suffixes in the
base name of the corpus). The particular slice used for evaluation is
specified using '--eval-set'.""")

  //// Eval output options
  var results_file =
    ap.option[String]("results-file", "results", "res",
      metavar = "FILE",
      help = """If specified, prefix of file to store results into.
Results are stored as a textdb database, i.e. two files will be written,
with extensions '.data.txt' and '.schema.txt', with the former storing the
data as tab-separated fields and the latter naming the fields.

See also '--print-results' for outputting human-readable results to stderr.""")

  var print_results =
    ap.flag("print-results", "show-results", "pr", "sr",
      help = """Show individual results for each test document. Use
'--num-top-cells-to-output' to control size of outputted tables.""")

  var print_results_as_list =
    ap.flag("print-results-as-list", "pral",
      help = """Also print individual results for each test document in a
list format that may be easier to parse. Use '--num-top-cells-to-output'
to control size of outputted list.  Implies '--print-results'.""")

  if (print_results_as_list)
    print_results = true

  var print_knn_results =
    ap.flag("print-knn-results", "show-knn-results", "pkr", "skr",
      help = """Also print kNN-related results for each test document.
Implies '--print-results'.""")

  if (print_knn_results)
    print_results = true

  var results_by_range =
    ap.flag("results-by-range", "rbr",
      help = """Show results by range (of error distances and number of
documents in correct cell).  Not on by default as counters are used for this,
and setting so many counters breaks some Hadoop installations.""")

  var num_nearest_neighbors =
    ap.option[Int]("num-nearest-neighbors", "knn", default = 4,
      must = be_>(0),
      help = """Number of nearest neighbors (k in kNN), if
'--print-knn-results'. Default is %default.""")

  var num_top_cells_to_output =
    ap.option[Int]("num-top-cells-to-output", "num-top-cells", "ntc",
      default = 5,
      help = """Number of top cells to output when '--print-results'; -1 means
output all. Default is %default.""")

  //// Restricting documents evaluated
  var oracle_results =
    ap.flag("oracle-results",
      help = """Only compute oracle results (much faster).""")

  var num_training_docs =
    ap.option[Int]("num-training-docs", "ntrain", metavar = "NUM",
      default = 0,
      must = be_>=(0),
      help = """Maximum number of training documents to use.
0 means no limit.  Default 0, i.e. no limit.""")

  var num_test_docs =
    ap.option[Int]("num-test-docs", "ntest", metavar = "NUM",
      default = 0,
      must = be_>=(0),
      help = """Maximum number of test (evaluation) documents to process.
0 means no limit.  Default 0, i.e. no limit.""")

  var skip_initial_test_docs =
    ap.option[Int]("skip-initial-test-docs", "skip-initial", metavar = "NUM",
      default = 0,
      must = be_>=(0),
      help = """Skip this many test docs at beginning.  Default 0, i.e.
don't skip any documents.""")

  var every_nth_test_doc =
    ap.option[Int]("every-nth-test-doc", "every-nth", metavar = "NUM",
      default = 1,
      must = be_>(0),
      help = """Only process every Nth test doc.  Default 1, i.e.
process all.""")
  //  def skip_every_n_test_docs =
  //    ap.option[Int]("skip-every-n-test-docs", "skip-n", default = 0,
  //      help = """Skip this many after each one processed.  Default 0.""")
}

trait GridLocateRankParameters {
  this: GridLocateParameters =>

  protected def ranker_default = "naive-bayes"
  protected def ranker_choices = Seq(
        Seq("full-kl-divergence", "full-kldiv", "full-kl"),
        Seq("partial-kl-divergence", "partial-kldiv", "partial-kl", "part-kl",
            "kl-divergence", "kldiv", "kl"),
        Seq("symmetric-full-kl-divergence", "symmetric-full-kldiv",
            "symmetric-full-kl", "sym-full-kl"),
        Seq("symmetric-partial-kl-divergence",
            "symmetric-partial-kldiv", "symmetric-partial-kl", "sym-part-kl"),
        Seq("partial-cosine-similarity", "partial-cossim", "part-cossim",
            "cosine-similarity", "cossim"),
        Seq("full-cosine-similarity", "full-cossim"),
        Seq("smoothed-partial-cosine-similarity", "smoothed-partial-cossim",
            "smoothed-part-cossim",
            "smoothed-cosine-similarity", "smoothed-cossim"),
        Seq("smoothed-full-cosine-similarity", "smoothed-full-cossim"),
        Seq("sum-frequency"),
        Seq("naive-bayes", "nb"),
        Seq("classifier"),
        Seq("hierarchical-classifier", "hier-classifier"),
        Seq("average-cell-probability", "avg-cell-prob", "acp"),
        Seq("salience", "internal-link"),
        Seq("random"),
        Seq("num-documents", "numdocs", "num-docs"))

  protected def ranker_non_baseline_help =
"""'full-kl-divergence' (or 'full-kldiv') searches for the cell where the KL
divergence between the document and cell is smallest.

'partial-kl-divergence' (or 'partial-kldiv', or simply 'kl-divergence' or
'kldiv') is similar but uses an abbreviated KL divergence measure that only
considers the words seen in the document; empirically, this appears to work
just as well as the full KL divergence.

'partial-cosine-similarity' (or 'partial-cossim', or simply 'cosine-similarity'
or 'cossim') does a cosine-similarity calculation between the document and
cell, treating their distributions as vectors. As with 'partial-kl-divergence',
this considers only the words seen in the document. This is most useful with
'--tf-idf', which transforms the probabilities using the TF-IDF algorithm.

'full-cosine-similarity' (or 'full-cossim') is similar but it considers words
in both document and cell.

'smoothed-partial-cosine-similarity' (or 'smoothed-partial-cossim', or simply
'smoothed-cosine-similarity' or 'smoothed-cossim') does a cosine-similarity
calculation using smoothed probabilities; the plain cosine-similarity
ranker uses unsmoothed probabilities.

'smoothed-full-cosine-similarity' (or 'smoothed-full-cossim') is similar to
'smoothed-partial-cosine-similarity', but considers words in both document
and cell (words in the cell but not the document will have non-zero
probability in the document in this case because of smoothing).

'sum-frequency' just adds up the unsmoothed probability calculations for
each word in the document. This is most useful in conjunction with '--tf-idf',
which transforms the probabilities using the TF-IDF algorithm.

'naive-bayes' uses the Naive Bayes algorithm to match a test document against
a training document (e.g. by assuming that the words of the test document
are independent of each other, if we are using a unigram language model).
The strategy for computing prior probability is specified using
'--naive-bayes-prior'. See also '--naive-bayes-prior-weight' for controlling
how the prior probability and likelihood (word probabilities) are weighted.

'classify' uses a linear classifier, treating each possible cell as a
separate class. This only works if the number of cells is small (not more
than a few hundred, probably).

'average-cell-probability' (or 'celldist') involves computing, for each word,
a probability distribution over cells using the language model of each cell,
and then combining the distributions over all words in a document, weighted by
the count the word in the document.

"""

  protected def ranker_baseline_help =
"""'salience' means use combined salience value of a cell. This is computed
by adding up the salience of the documents in a cell. Only some corpora
provide salience values for documents; e.g. for Wikipedia, this is the
number of incoming links pointing to a document (article) from other articles.

'random' means choose randomly.

'num-documents' (or 'num-docs' or 'numdocs'; only in cell-type matching) means
use number of documents in cell.

"""

  var ranker =
    ap.option[String]("s", "ranker", "ra", "strategy",
      default = ranker_default,
      aliasedChoices = ranker_choices,
      help = """Ranking strategy/strategies to use for geolocation.
""" + ranker_non_baseline_help +
"""In addition, the following "baseline" probabilities exist, which use
simple algorithms meant for comparison purposes.

""" + ranker_baseline_help +
"""Default is '%default'.""")

  //// Options used when doing Naive Bayes geolocation
//FIXME: No longer clear what the idea of the following was.
//  var naive_bayes_word_weighting =
//    ap.option[String]("naive-bayes-word-weighting", "nbw",
//      metavar = "STRATEGY",
//      default = "fixed",
//      choices = Seq("fixed", "distance-weighted"),
//      help = """Strategy for weighting the different probabilities
//that go into Naive Bayes.  If 'fixed', use fixed weights for the prior
//probability and likelihood, according to '--naive-bayes-prior-weight'.
//If 'distance-weighted' weight each word according to distance from the
//toponym (????).""")
  var naive_bayes_prior_weight =
    ap.option[Double]("naive-bayes-prior-weight", "nbpw",
      metavar = "WEIGHT",
      default = 0.5,
      must = be_and(be_>=(0.0), be_<=(1.0)),
      help = """Relative weight to assign to the prior probability (vs.
the likelihood) when doing Naive Bayes ranking.  Default 0.5, which does
standard unweighted Naive Bayes.""")
  var naive_bayes_prior =
    ap.option[String]("naive-bayes-prior", "nbp",
      metavar = "STRATEGY",
      default = "uniform",
      choices = Seq("uniform", "salience", "log-salience", "num-docs",
        "log-num-docs"),
      help = """Strategy for computing the prior probability when doing
Naive Bayes ranking. Possibilities are 'uniform', 'salience', 'log-salience',
'num-docs', and 'log-num-docs'. Default %default.""")
  var naive_bayes_features =
    ap.option[String]("naive-bayes-features", "nbf",
      metavar = "STRATEGY",
      default = "terms",
      help = """Which features to use when using Naive Bayes to rank
the cells for a document. Possibilities are:

'terms' (use the terms of the language model);

'rough-ranker' (run a ranker -- usually at a coarser grid partition -- and
  use its scores as a feature);

Multiple feature types can be specified, separated by spaces or commas.

Default %default.""")

  lazy val naive_bayes_feature_list =
    get_feature_list(naive_bayes_features, Seq("terms", "rough-ranker"),
      "naive-bayes-features")

  var rough_ranker_args =
    ap.option[String]("rough-ranker-args", "rra",
      default = "",
      help = """Arguments to use when running the rough ranker that computes
a sort of prior value in rough-to-fine ranking, for the fine Naive-Bayes
ranker. This is for use with '--naive-bayes-features rough-ranker'.""")

  var num_levels =
    ap.option[Int]("num-levels", "nl",
      metavar = "LEVELS",
      default = 2,
      must = be_>(0),
      help = """Number of levels when doing hierarchical ranking.
Default %default.""")

  var beam_size =
    ap.option[Int]("beam-size", "bs",
      metavar = "SIZE",
      default = 10,
      must = be_>(0),
      help = """Number of top-ranked items to keep at each level when
doing hierarchical ranking. Default %default.""")
}

trait GridLocateFeatureParameters {
  this: GridLocateParameters =>

  var classifier =
    ap.option[String]("classifier", "cl",
      default = "tadm",
      choices = Seq("random", "oracle", "perceptron", "avg-perceptron",
        "pa-perceptron", "cost-perceptron", "mlogit", "tadm", "vowpal-wabbit",
        "cost-vowpal-wabbit"),
      help = """Type of classifier to use when '--ranker=classify'.
Possibilities are:

'random' (pick randomly among the cells to rerank);

'oracle' (always pick the correct cell);

'perceptron' (using a basic-algorithm perceptron, try to maximimize the
  likelihood of picking the correct cell in the training set);

'avg-perceptron' (similar to 'perceptron' but averages the weights from
  the various rounds of the perceptron computations -- this usually improves
  results if the weights oscillate around a certain error rate, rather than
  steadily improving);

'pa-perceptron' (passive-aggressive perceptron, which usually leads to steady
  but gradually dropping-off error rate improvements with increased number
  of rounds);

'cost-perceptron' (cost-sensitive passive-aggressive perceptron, using the
  error distance as the cost);

'mlogit' (use R's mlogit() function to implement a multinomial conditional
  logit aka label-specific maxent model, a type of generalized linear model);

'tadm' (use TADM to implement a label-specific maxent or other model);

'vowpal-wabbit' (use VowpalWabbit to implement a maxent or other model);

'cost-vowpal-wabbit' (use VowpalWabbit to implement a cost-sensitive maxent
  or other model).

Default is '%default'.

For the perceptron classifiers, see also `--pa-variant`,
`--perceptron-error-threshold`, `--perceptron-aggressiveness` and
`--iterations`.""")

  protected def with_binned(feats: String*) =
    feats flatMap { f => Seq(f, f + "-binned") }

  val features_simple_doc_only_gram_choices =
    (Seq("binary") ++ with_binned("doc-count", "doc-prob")
    ).map { "gram-" + _ }

  val features_simple_doc_cell_gram_choices =
    with_binned("cell-count", "doc-prob").map { "gram-" + _ }

  val features_matching_gram_choices =
    (Seq("binary") ++ with_binned(
      "doc-count", "cell-count", "doc-prob", "cell-prob",
      "count-product", "count-quotient", "prob-product", "prob-quotient",
      "kl")
    ).map { "matching-gram-" + _ }

  val features_all_gram_choices =
    features_simple_doc_only_gram_choices ++
    features_simple_doc_cell_gram_choices ++
    features_matching_gram_choices

  def allowed_features =
    Seq("misc", "types-in-common", "model-compare", "trivial") ++
    features_all_gram_choices

  var classify_features =
    ap.option[String]("classify-features", "cf",
      default = "misc",
      help = """Which features to use when using a linear classifier to rank
the cells for a document. Possibilities are:
""" + classify_features_help + """
Multiple feature types can be specified, separated by spaces or commas.

Default %default.""")

  def classify_features_help = """
'trivial' (no features, for testing purposes);

'gram-binary' (when a gram -- i.e. word or ngram according to the type of
  language model -- exists in the document, create a feature with the value
  1);

'gram-doc-count' (when a gram exists in the document, create a feature with
  the document gram count as the value);

'gram-cell-count' (when a gram exists in the document, create a feature with
  the cell gram count as the value);

'gram-doc-prob' (when a gram exists in the document, create a feature with
  the document gram probability as the value);

'gram-cell-prob' (when a gram exists in the document, create a feature with
  the cell gram probability as the value);

'matching-gram-*' (same as 'gram-*' but create a feature only when the gram --
  i.e. word or ngram according to the type of language model -- exists in
  both document and cell);

'matching-gram-count-product' (use the product of the document and cell
  gram count when a gram exists in both document and cell);

'matching-gram-count-quotient' (use the quotient of the cell and document
  gram count when a gram exists in both document and cell);

'matching-gram-prob-product' (use the product of both the document and cell
  gram probability when a gram exists in both document and cell);

'matching-gram-prob-quotient' (use the quotient of the cell and document
  gram probability when a gram exists in both document and cell);

'matching-gram-kl' (when a gram exists in both document and cell, use the
  individual KL-divergence component score between document and cell for the
  gram, else 0);

'*-binned' (for all feature types given above except for the '*-binary'
  types, a "binned" equivalent exists that creates binary features for
  different ranges (aka bins) of values, generally logarithmically, but
  by fixed increments for fractions and similarly bounded, additive values

'types-in-common' (number of word/ngram types in the document that are also
  found in the cell)

'model-compare' (various ways of comparing the language models of document and
  cell: KL-divergence, symmetric KL-divergence, cosine similarity, Naive Bayes)

'misc' (other non-word-specific features, e.g. number of documents in a cell,
  number of word types/tokens in a cell, etc.; should be fairly fast
  to compute)
"""

  lazy val classify_feature_list =
    get_feature_list(classify_features, allowed_features,
      "classify-features")

  var classify_binning =
    ap.option[String]("classify-binning", "cb", metavar = "BINNING",
      default = "also",
      aliasedChoices = Seq(
        Seq("also", "yes"),
        Seq("only"),
        Seq("no")),
      help = """Whether to include binned features in addition to or in place
of numeric features. If 'also' or 'yes', include both binned and numeric
features. If 'only', include only binned features. If 'no', include only
numeric features. Binning features involves converting numeric values to one
of a limited number of choices. Binning of most values is done logarithmically
using base 2. Binning of some fractional values is done in equal intervals.
Default '%default'.

NOTE: Currently, this option does not affect any of the gram-by-gram feature
types, which have separate '*-binned' equivalents that can be specified
directly. It does affect 'misc', 'model-compare', 'rank-score' and similar
non-word-by-word feature types. See '--classify-features'.""")

  var initial_weights =
    ap.option[String]("initial-weights", "iw",
      default = "zero",
      choices = Seq("zero", "rank-score-only", "random"),
      help = """How to initialize weights during reranking. Possibilities
are 'zero' (set to all zero), 'rank-score-only' (set to zero except for
the original ranking score, when reranking), 'random' (set to random).""")

  var random_restart =
    ap.option[Int]("random-restart", "rr",
      default = 1,
      must = be_>(0),
      help = """How often to recompute the weights. The resulting
set of weights will be averaged. This only makes sense when
'--initialize-weights random', and implements random restarting.
NOT CURRENTLY IMPLEMENTED.""")
}

trait GridLocateRerankParameters {
  this: GridLocateParameters with GridLocateFeatureParameters =>

  var reranker =
    ap.option[String]("reranker", "rer",
      default = "none",
      choices = Seq("none", "random", "oracle", "perceptron", "avg-perceptron",
        "pa-perceptron", "cost-perceptron", "mlogit", "tadm", "vowpal-wabbit"),
      help = """Type of strategy to use when reranking.  Possibilities are:

'none' (no reranking);

'random' (pick randomly among the candidates to rerank);

'oracle' (always pick the correct candidate if it's among the candidates);

'perceptron' (using a basic-algorithm perceptron, try to maximimize the
  likelihood of picking the correct candidate in the training set);

'avg-perceptron' (similar to 'perceptron' but averages the weights from
  the various rounds of the perceptron computations -- this usually improves
  results if the weights oscillate around a certain error rate, rather than
  steadily improving);

'pa-perceptron' (passive-aggressive perceptron, which usually leads to steady
  but gradually dropping-off error rate improvements with increased number
  of rounds);

'cost-perceptron' (cost-sensitive passive-aggressive perceptron, using the
  error distance as the cost);

'mlogit' (use R's mlogit() function to implement a multinomial conditional
  logit aka maxent ranking model, a type of generalized linear model);

'tadm' (use TADM to implement a maxent or other ranking model);

'vowpal-wabbit' (use VowpalWabbit to implement a maxent or other ranking model).

Default is '%default'.

For the perceptron optimizers, see also `--pa-variant`,
`--perceptron-error-threshold`, `--perceptron-aggressiveness` and
`--iterations`.""")

  var rerank_features =
    ap.option[String]("rerank-features", "rf",
      default = "rank-score,misc",
      help = """Which features to use in the reranker, to characterize the
similarity between a document and candidate cell (largely based on the
respective language models). Possibilities are 'rank-score' (use the
original rank and ranking score), plus all the possible feature types of
'--classify-features'.

Default %default.

Note that this only is used when '--reranker' specifies something other than
'none', 'random' or 'oracle'.""")

  val allowed_rerank_features =
    allowed_features ++ Seq("rank-score")

  lazy val rerank_feature_list =
    get_feature_list(rerank_features, allowed_rerank_features,
      "rerank-features")

  var rerank_binning =
    ap.option[String]("rerank-binning", "rb", metavar = "BINNING",
      default = "also",
      aliasedChoices = Seq(
        Seq("also", "yes"),
        Seq("only"),
        Seq("no")),
      help = """Whether to include binned features in addition to or in place
of numeric features in the reranker. This works identically to
'--classify-binning'.""")

  var rerank_top_n =
    ap.option[Int]("rerank-top-n", "rtn",
      default = 10,
      must = be_>(0),
      help = """Number of top-ranked items to rerank.  Default is %default.""")

  var rerank_num_training_splits =
    ap.option[Int]("rerank-num-training-splits", "rnts",
      default = 5,
      must = be_>(0),
      help = """Number of splits to use when training the reranker.
The source training data is split into this many segments, and each segment
is used to construct a portion of the actual training data for the reranker
by creating an initial ranker based on the remaining segments.  This is
similar to cross-validation for evaluation purposes and serves to avoid the
problems that ensue when a given piece of data is evaluated on a machine
trained on that same data. Default is %default.""")

  var rerank_lang_model =
    ap.optionWithParams[String]("rerank-lang-model", "rerank-word-dist",
        "rlm", "rwd",
      default = ("default", ""),
      aliasedChoices = lang_model_choices :+ Seq("default"),
      help = """Language model for reranking. See `--lang-model`.

A value of 'default' means use the same lang model as is specified in
`--lang-model`. Default value is '%default'.""")

  if (rerank_lang_model._1 == "default")
    rerank_lang_model = lang_model

  lazy val rerank_lang_model_type = {
    if (reranker == "none")
      grid_lang_model_type
    else if (rerank_lang_model == (("unsmoothed-ngram", ""))) "ngram"
    else "unigram"
  }

  var rerank_interpolate =
    ap.option[String]("rerank-interpolate",
      default = "default",
      aliasedChoices = Seq(
        Seq("yes", "interpolate"),
        Seq("no", "backoff"),
        Seq("default")),
      help = """Whether to do interpolation rather than back-off when
reranking. See `--interpolate`.""")
}

trait GridLocateOptimizerParameters {
  this: GridLocateParameters with GridLocateFeatureParameters =>
  var iterations =
    ap.option[Int]("iterations", "it",
      metavar = "INT",
      default = 10000,
      must = be_>(0),
      help = """Maximum number of iterations (rounds) when training a
perceptron or TADM model (default: %default).""")

  var pa_variant =
    ap.option[Int]("pa-variant",
      metavar = "INT",
      default = 1,
      choices = Seq(0, 1, 2),
      help = """For passive-aggressive perceptron when reranking: variant
(0, 1, 2; default %default).""")

  var pa_cost_type =
    ap.option[String]("pa-cost-type",
      default = "prediction-based",
      choices = Seq("prediction-based", "max-loss"),
      help = """For passive-aggressive cost-sensitive perceptron when reranking:
type of algorithm used ('prediction-based' or 'max-loss').
Performance is generally similar, although the prediction-based algorithm
may be faster because it only needs to evaluate the cost function at most
once per round per training instance, where the max-loss algorithm needs
to evaluate the cost for each possible label (in the case of reranking, this
means as many times as the value of `--rerank-top-n`). Default %s.""")

  var perceptron_error_threshold =
    ap.option[Double]("perceptron-error-threshold",
      metavar = "DOUBLE",
      default = 1e-10,
      must = be_>(0.0),
      help = """For perceptron when reranking: Total error threshold below
which training stops (default: %default).""")

  var perceptron_aggressiveness =
    ap.option[Double]("perceptron-aggressiveness",
      metavar = "DOUBLE",
      default = 0.0,
      must = be_>=(0.0),
      help = """For perceptron: aggressiveness factor > 0.0.  If 0, use
the default value, currently 1.0 for both the the regular and PA-perceptrons,
but that may change because the meaning is rather different for the two
types.  For the regular perceptron, the meaning of this factor is literally
"aggressiveness" in that weight changes are directly scaled by that factor.
For the PA-perceptron, it should be interpreted more as non-restrictiveness
than aggressiveness, and applies only to variants 1 and 2. For both variants,
a setting of infinity means "completely non-restricted" and makes the
algorithm equivalent to variant 0, which is always non-restricted.  The
paper of Crammer et al. (2006) suggests that, at least for some applications,
values of the parameter (called "C") at C = 100 have very little restriction,
while C = 0.001 has a great deal of restriction and is useful with notably
noisy training data.  We choose C = 1 as a compromise.""")

  if (perceptron_aggressiveness == 0.0) // If default ...
    // Currently same for both regular and pa-perceptron, despite
    // differing interpretations.
    perceptron_aggressiveness = 1.0

  var perceptron_decay =
    ap.option[Double]("perceptron-decay",
      metavar = "DOUBLE",
      default = 0.0,
      must = be_within(0.0, 1.0),
      help = """Amount by which to decay the perceptron aggressiveness
factor each round. For example, the value 0.01 means to decay the factor
by 1% each round. This should be a small number, and always a number
between 0 and 1.""")

  var gaussian_penalty =
    ap.option[Double]("gaussian-penalty", "gaussian", "l2",
      metavar = "PENALTY",
      default = 0.0,
      must = be_>=(0),
      help = """If specified, impose a Gaussian (L2) penalty function
(aka ridge regression, Tikhonov regularization) with the specified value
(smaller = more penalty??). If zero (the default), don't specify any penalty.
This applies when using TADM and Vowpal Wabbit.""")

  var lasso_penalty =
    ap.option[Double]("lasso-penalty", "lasso", "l1",
      metavar = "PENALTY",
      default = 0.0,
      must = be_>=(0),
      help = """If specified, impose a Lasso (L1) penalty function with
the specified value (smaller = more penalty??). If zero (the default),
don't specify any penalty. This applies when using TADM and Vowpal Wabbit.""")

  var tadm_method =
    ap.option[String]("tadm-method",
      metavar = "METHOD",
      default = "tao_lmvm",
      choices = Seq("tao_lmvm", "tao_cg_prp", "iis", "gis", "steep",
        "perceptron"),
      help = """Optimization method in TADM: One of 'tao_lmvm', 'tao_cg_prp',
'iis', 'gis', 'steep', 'perceptron'. Default '%default'.""")

  var tadm_uniform_marginal =
    ap.flag("tadm-uniform-marginal",
      help = """If specified, use uniform rather than pseudo-likelihood
marginal calculation in TADM.""")

  var vw_loss_function =
    ap.option[String]("vw-loss-function", "vlf",
      default = "logistic",
      choices = Seq("logistic", "hinge", "squared", "quantile"),
      help = """Loss function for Vowpal Wabbit: One of 'logistic'
(do logistic regression), 'hinge' (use hinge loss, as for an SVM),
'squared' (do least-squares linear regression), 'quantile'
(do quantile regression, trying to predict the median or some
other quantile, rather than the mean as with least squares).""")

  var vw_multiclass =
    ap.option[String]("vw-multiclass", "vm",
      default = "oaa",
      choices = Seq("oaa", "ect", "wap"),
      help = """In Vowpal Wabbit, how to reduce a multiclass problem to a
set of binary problems: One of 'oaa' (one against all), 'ect' (error-correcting
tournament -- only for plain 'vowpal-wabbit'), 'wap' (weighted all pairs --
only for 'cost-vowpal-wabbit').""")
  if (classifier == "vowpal-wabbit" && vw_multiclass == "wap")
    ap.error("--vw-multiclass wap not possible with --classifier vowpal-wabbit")
  else if (classifier == "cost-vowpal-wabbit" && vw_multiclass == "ect")
    ap.error("--vw-multiclass ect not possible with --classifier cost-vowpal-wabbit")

  var vw_cost_function =
    ap.option[String]("vw-cost-function", "vcf",
      default = "dist",
      choices = Seq("dist", "sqrt-dist", "log-dist"),
      help = """How to compute the cost in 'cost-vowpal-wabbit'. Possibilities:
'dist' (use the error distance directly), 'sqrt-dist' (use the square root
of the distance), 'log-dist' (use the log of the distance).""")

  var save_vw_model =
    ap.option[String]("save-vw-model", "svm",
      default = "",
      help = """Save the created Vowpal Wabbit model to the specified filename.
If unspecified, a temporary file will be created to hold the model, and
deleted upon exit unless '--debug preserve-tmp-files' is given. When
doing hierarchical classification, only the top-level model is saved
(see '--save-vw-submodels').""")

  var save_vw_submodels =
    ap.option[String]("save-vw-submodels", "svsm",
      default = "",
      help = """Save the created Vowpal Wabbit submodels to the specified
filename during hierarchical classification. The filename should have %l
and %i in it, which will be replaced by the level and classifier index,
respectively.  If unspecified, temporary files will be created to hold
the submodels, and deleted upon exit unless '--debug preserve-tmp-files'
is given.""")

  var load_vw_model =
    ap.option[String]("load-vw-model", "lvm",
      default = "",
      help = """If specified, load a previously trained Vowpal Wabbit model
from the given filename instead of training a new model.""")

  var load_vw_submodels =
    ap.option[String]("load-vw-submodels", "lvsm",
      default = "",
      help = """If specified, load previously trained Vowpal Wabbit submodels
from the given filename during hierarchical classification, instead of
training a new model. The filename should have %l and %i in it, which will
be replaced by the level and classifier index, respectively.""")

  var vw_args =
    ap.option[String]("vw-args",
      default = "",
      help = """Miscellaneous arguments to pass to Vowpal Wabbit at training
time.""")

  var nested_vw_args =
    ap.option[String]("nested-vw-args",
      help = """Miscellaneous arguments to pass to Vowpal Wabbit at training
time, for levels other than the top one in a hierarchical classifier.
Defaults to the same arguments as '--vw-args'.""")

  var fallback_vw_args =
    ap.option[String]("fallback-vw-args",
      default = "",
      help = """HACK! If Vowpal Wabbit fails to produce a model using the
arguments in '--vw-args' or '--nested-vw-args', fall back to using the
arguments here, which should always work.""")
}

trait GridLocateMiscParameters {
  this: GridLocateParameters =>

  //// Miscellaneous options for controlling internal operation
  var no_parallel =
    ap.flag("no-parallel",
      help = """If true, don't do ranking computations in parallel.""")

  var max_time_per_stage =
    ap.option[Double]("max-time-per-stage", "mts", metavar = "SECONDS",
      default = 0.0,
      must = be_>=(0.0),
      help = """Maximum time per stage in seconds.  If 0, no limit.
Used for testing purposes.  Default 0, i.e. no limit.""")

  // Named `debug_specs` not just `debug` to avoid possible conflict with
  // the `debug` in package `debug`.
  var debug_specs =
    ap.multiOption[String]("d", "debug", metavar = "FLAGS",
      help = """Output debug info, or control operation of certain parts of
the program. The parameter value consists of one or more debug parameters,
indicating different actions to take. Separate parameters by spaces, commas
or semicolons.  Params can be boolean, if given alone, or valueful, if given
as PARAM=VALUE.  Certain params are list-valued; multiple values are specified
by including the parameter multiple times, or by separating values by a colon.

Multiple occurrences of `--debug` can also occur, and the debug specs from
all occurrences of the command line argument are combined together.

Note that debug parameters that change the operation of the program should
probably be made regular parameters. Sometimes they are debug parameters
because they are (or were) provisional.

The best way to figure out the possible parameters is by reading the
source code. (Look for references to debug("foo") for boolean params,
debugval("foo")/debugint("foo")/debuglong("foo")/debugdouble("foo") for
valueful params, or debuglist("foo") for list-valued params.)

Some known debug flags:

--------------------- classifier-related ---------------------

parallel-hier-classifier: Train hierarchical classifiers at level >= 2 in
  parallel.

hier-classifier: Output debug info about the operation of the hierarchical
  classifier when training.

features: Output document features for classifier trainers.

weights: Output weights learned by non-VowpalWabbit classifier trainers.

vw-normalize: Normalize the raw predictions coming from a VW classifier.
  Mostly affects the hierarchical classifier with beam size > 1.

vw-daemon: Document me.

preserve-tmp-files: Don't delete the temporary files created for VW upon
  program exit.

preserve-vw-daemon: Don't kill the VW daemon used in daemon mode upon
  progrma exit.

warn-on-bad-model:

export-instances:

perceptron:
weights-each-iteration:
training-data:
feature-relevance:
multilabel:

--------------------- ranker-related ---------------------

gridrank: For the given test document number (starting at 1), output
  a grid of the predicted rank for cells around the correct cell.
  Multiple documents can have the rank output, e.g. --debug 'gridrank=45:58'
  (This will output info for documents 45 and 58.) This output can be
  postprocessed to generate nice graphs; this is used e.g. in Wing's thesis.

gridranksize: Size of the grid, in numbers of documents on a side.
  This is a single number, and the grid will be a square centered on the
  correct cell. (Default currently 11.)

kldiv: Print out words contributing most to KL divergence.

relcontribgrams: Print out words contributing most to the choice of the
  top-ranked cell vs. other cells. This is computed as if the Naive Bayes
  algorithm were being used. We compare cell at rank 1 vs. cell at rank 2,
  and cell at rank 1 vs. other cells, individually for each word.

commontop: Extra info for debugging
  --baseline-ranker=salience-most-common-toponym.

rescale-scores:
negate-scores:
rerank-problems:
rescale-features:
rerank-training:

--------------------- language model related ---------------------

pretend-words-seen-once:
global-backoff-stats:
test-kl:
dunning:

--------------------- cell/grid-related ---------------------

cell: Print out info on each cell of the Earth as it's generated.  Also
  triggers some additional info during toponym resolution. (Document me.)

kd-tree-grid: Document me.

describe-kd-tree: Document me.

--------------------- os-related ---------------------

compare-cpu-time-methods:
compare-memory-usage-methods:

--------------------- exception-related ---------------------

no-catch:
stack-trace, stacktrace:

--------------------- misc ---------------------

wordcountdocs: Regenerate document file, filtering out documents not
  seen in any counts file.

pcl-travel: Extra info for debugging --eval-format=pcl-travel.

no-evaluation:

""")

  if (debug_specs != null)
    debug_specs.map { parse_debug_spec(_) }
}

/**
 * General class retrieving command-line arguments or storing programmatic
 * configuration parameters for a cell-grid-based application. The
 * parameters in here are those necessary for initializing a cell grid
 * from training documents, but not those used for geolocating test
 * documents or other applications (e.g. creating KML maps of the
 * distribution of the training docs).
 */
trait GridLocateParameters extends ArgParserParameters with
  GridLocateBasicParameters with GridLocateLangModelParameters with
  GridLocateCellParameters with GridLocateEvalParameters with
  GridLocateRankParameters with GridLocateFeatureParameters with
  GridLocateRerankParameters with GridLocateOptimizerParameters with
  GridLocateMiscParameters {
  def get_feature_list(featstring: String, allowed: Seq[String],
      arg: String) = {
    val features = featstring.split("[ ,]")
    for (feature <- features) {
      if (!(allowed contains feature))
        ap.usageError(s"Unrecognized feature '$feature' in --$arg")
    }
    features.toSeq
  }
}

/**
 * Driver class for creating cell grids over some coordinate space, with a
 * language model associated with each cell and initialized from a corpus
 * of documents by concatenating all documents located within the cell.
 *
 * Driver classes like this have `run` to do the main operation.
 * A subclass of GridLocateApp is often used to wrap the driver and
 * initialize parameters from the command line.
 */
trait GridLocateDriver[Co] extends HadoopableArgParserExperimentDriver {
  override type TParam <: GridLocateParameters

  // NOTE: `verbose` is ignored in favor of params.verbose; present only
  // because we can't create a show_progress without it, would have to
  // rename.
  override def show_progress(verb: String, item_name: String,
      verbose: MsgVerbosity = MsgNormal, secs_between_output: Double = 15,
      maxtime: Double = 0.0, maxitems: Int = 0
    ) = super.show_progress(verb, item_name,
      verbose = if (params.verbose) MsgVerbose else MsgNormal,
      secs_between_output = secs_between_output, maxtime = maxtime,
      maxitems = maxitems)

  def deserialize_coord(coord: String): Co

  /**
   * Field in textdb corpus used to access proper type of lang model.
   */
  def grid_word_count_field = {
    if (params.grid_lang_model_type == "ngram")
      "ngram-counts"
    else
      "unigram-counts"
  }

  def rerank_word_count_field = {
    if (params.rerank_lang_model_type == "ngram")
      "ngram-counts"
    else
      "unigram-counts"
  }

  /**
   * Suffix to pass when locating/reading files from a textdb database of
   * documents.
   */
  def document_textdb_suffix = "-" + params.eval_set

  val stopwords_file_in_tg = "src/main/resources/data/%s/stopwords.txt"

  // Read in the list of stopwords from the given filename.
  protected def read_stopwords_from_file(filehand: FileHandler,
      stopwords_filename: String, language: String) = {
    def compute_stopwords_filename(filename: String) = {
      if (filename != null) filename
      else {
        val tgdir = TextGrounderInfo.get_textgrounder_dir
        // Concatenate directory and rest in most robust way
        filehand.join_filename(tgdir, stopwords_file_in_tg format language)
      }
    }
    val filename = compute_stopwords_filename(stopwords_filename)
    if (params.verbose)
      errprint("Reading stopwords from %s...", filename)
    filehand.openr(filename).toSet
  }

  protected def read_stopwords = {
    read_stopwords_from_file(get_file_handler, params.stopwords_file,
      params.language)
  }

  lazy val the_stopwords = {
    if (params.no_stopwords) Set[String]()
    else read_stopwords
  }

  protected def read_whitelist = {
    val filehand = get_file_handler
    val wfn = params.whitelist_file
    if(wfn == null || wfn.length == 0)
      Set[String]()
    else
      filehand.openr(wfn).toSet
  }

  protected def read_whitelist_weight_file = {
    if (params.whitelist_weight_file != null) {
      val word_weights = mutable.Map[String,Double]()
      if (params.verbose)
        errprint("Reading whitelist weights...")
      var numwords = 0
      for (row <- get_file_handler.openr(params.whitelist_weight_file)) {
        val Array(word, weightstr) = row.split("\t")
        val weight_1 = weightstr.toDouble
        val weight = if (params.weight_abs) weight_1.abs else weight_1
        if (word_weights contains word)
          warning("Word %s with weight %s already seen with weight %s",
            word, weight, word_weights(word))
        else {
          numwords += 1
          if (weight >= params.weight_cutoff_value)
            word_weights(word) = weight
        }
      }
      if (params.weight_cutoff_percent == 100.0)
        word_weights.keys.toSet
      else {
        val sorted_word_weights = word_weights.toSeq.sortBy(-_._2)
        val num_to_take =
          (params.weight_cutoff_percent / 100.0 * numwords).toInt
        sorted_word_weights.take(num_to_take).map(_._1).toSet
      }
    } else Set[String]()
  }

  lazy protected val the_whitelist =
    read_whitelist ++ read_whitelist_weight_file

  protected def read_word_weight_file = {
    if (params.word_weight_file != null) {
      val word_weights = mutable.Map[Gram,Double]()
      if (params.verbose)
        errprint("Reading word weights...")
      var numwords = 0
      for (row <- get_file_handler.openr(params.word_weight_file)) {
        val Array(word, weightstr) = row.split("\t")
        val weight_1 = weightstr.toDouble
        val weight = if (params.weight_abs) weight_1.abs else weight_1
        val wordint = Unigram.to_index(word)
        if (word_weights contains wordint)
          warning("Word %s with weight %s already seen with weight %s",
            word, weight, word_weights(wordint))
        else if (weight < 0)
          warning("Word %s with invalid negative weight %s",
            word, weight)
        else {
          numwords += 1
          if (weight >= params.weight_cutoff_value)
            word_weights(wordint) = weight
        }
      }
      val chopped_word_weights =
        if (params.weight_cutoff_percent == 100.0)
          word_weights
        else {
          val sorted_word_weights = word_weights.toSeq.sortBy(-_._2)
          val num_to_take =
            (params.weight_cutoff_percent / 100.0 * numwords).toInt
          sorted_word_weights.take(num_to_take).toMap
        }
      // Scale the word weights to have an average of 1.
      val values = chopped_word_weights.values
      val avg = values.sum / values.size
      assert_>(avg, 0, "",
        "Must have at least one non-ignored non-zero weight")
      val scaled_word_weights = chopped_word_weights map {
        case (word, weight) => (word, weight / avg)
      }
      val missing_word_weight =
        if (params.missing_word_weight >= 0)
          params.missing_word_weight / avg
        else
          1.0
      if (params.verbose)
        errprint("Reading word weights... done.")
      (scaled_word_weights, missing_word_weight)
    } else
      (mutable.Map[Gram,Double](), 0.0)
  }

  lazy val word_weights = read_word_weight_file

  /** Return a function that will create a LangModelBuilder object,
   * given a LangModelFactory.
   *
   * Currently there are two factory-type objects for language models
   * (language models): LangModelFactory (a lower-level factory to directly
   * create LangModel objects and handle details of initializing smoothing
   * models and such) and LangModelBuilder (a high-level factory that
   * knows how to create and initialize LangModels from source data,
   * handling issues like stopwords, vocabulary filtering, etc.). The two
   * factory objects need pointers to each other, and to handle this
   * needing mutable vars, one needs to create the other in its builder
   * function. So, rather than creating a LangModelBuilder ourselves, we
   * pass in a function to create one when creating a LangModelFactory.
   */
  protected def get_lang_model_builder_creator(lang_model_type: String,
      word_weights: collection.Map[Gram, Double],
      missing_word_weight: Double
    ) =
    (factory: LangModelFactory) => {
      if (lang_model_type == "ngram")
        new DefaultNgramLangModelBuilder(
          factory,
          ignore_case = !params.preserve_case_words,
          stopwords = the_stopwords,
          whitelist = the_whitelist,
          minimum_word_count = params.minimum_word_count,
          max_ngram = params.max_ngram,
          raw_text_max_ngram = params.raw_text_max_ngram)
      else
        new DefaultUnigramLangModelBuilder(
          factory, !params.preserve_case_words, the_stopwords, the_whitelist,
          params.minimum_word_count, word_weights, missing_word_weight)
    }

  /**
   * Create a LangModelFactory object of the appropriate kind given
   * command-line parameters. This is a factory for creating language models.
   */
  protected def create_lang_model_factory(lang_model_type: String,
      lm_spec: (String, String), interpolate: String,
      word_weights: collection.Map[Gram, Double],
      missing_word_weight: Double
  ) = {
    val create_builder = get_lang_model_builder_creator(lang_model_type,
      word_weights, missing_word_weight)
    val (lm, lmparams) = lm_spec
    if (lm == "unsmoothed-ngram")
      new UnsmoothedNgramLangModelFactory(create_builder)
    else if (lm == "dirichlet") {
      val dirichlet_factor = params.parser.parseSubParams(lm, lmparams,
        default = params.dirichlet_factor_default)
      if (dirichlet_factor < 0.0)
        param_error("Dirichlet factor must be >= 0, but is %g"
          format dirichlet_factor)
      new DirichletUnigramLangModelFactory(create_builder,
        interpolate, params.tf_idf, dirichlet_factor)
    }
    else if (lm == "jelinek-mercer") {
      val jelinek_factor = params.parser.parseSubParams(lm, lmparams,
        default = params.jelinek_factor_default)
      if (jelinek_factor < 0.0 || jelinek_factor > 1.0)
        param_error("Jelinek factor must be between 0.0 and 1.0, but is %g"
          format jelinek_factor)
      new JelinekMercerUnigramLangModelFactory(create_builder,
        interpolate, params.tf_idf, jelinek_factor)
    }
    else {
      if (lmparams.contains(':'))
        param_error("Parameters not allowed for pseudo-Good-Turing")
      new PseudoGoodTuringUnigramLangModelFactory(create_builder,
        interpolate, params.tf_idf)
    }
  }

  /**
   * Create a DocLangModelFactory object holding the LangModelFactory
   * objects needed by a document. Currently there may be two if
   * ranking and reranking require different dists.
   */
  protected def create_doc_lang_model_factory(
      word_weights: collection.Map[Gram, Double],
      missing_word_weight: Double
  ) = {
    val grid_lang_model_factory =
      create_lang_model_factory(params.grid_lang_model_type, params.lang_model,
        params.interpolate, word_weights, missing_word_weight)
    val rerank_lang_model_factory =
      if (params.grid_lang_model_type == params.rerank_lang_model_type)
        grid_lang_model_factory
      else
        create_lang_model_factory(params.rerank_lang_model_type,
          params.rerank_lang_model, params.rerank_interpolate,
          word_weights, missing_word_weight)
    new DocLangModelFactory(grid_lang_model_factory, rerank_lang_model_factory)
  }

  /**
   * Create a document factory (GridDocFactory) for creating documents
   * (GridDoc), given factory for creating the language models (language
   * models) associated with the documents.
   */
  protected def create_document_factory(
      lang_model_factory: DocLangModelFactory): GridDocFactory[Co]

  /**
   * Create a document factory (GridDocFactory) from command-line parameters.
   */
  def create_document_factory_from_params = {
    val (weights, mww) = word_weights
    val lang_model_factory = create_doc_lang_model_factory(weights, mww)
    create_document_factory(lang_model_factory)
  }

  /**
   * Create an empty cell grid (Grid) given a function to create a
   * document factory (in case we need to create multiple such factories).
   */
  protected def create_empty_grid(create_docfact: => GridDocFactory[Co],
    id: String): Grid[Co]

  /**
   * Read the raw training documents as a series of streams. Each stream is
   * used to generate a separate grid. This uses the values of the parameters
   * to determine where to read the documents from and how many documents to
   * read.  A "raw document" is simply an encapsulation of the fields used to
   * create a document (as read directly from the corpus), along with the
   * schema describing the fields.
   *
   * @return Iterable over pairs of an ID identifying the stream and a
   *   function to generate an iterator over the raw documents. The function
   *   takes a single argument, a string identifying the name of the logical
   *   operation that is processing the corpus documents, to be displayed in
   *   progress messages as the documents are read and processed. An example
   *   operation might simply be "reading", which will produce the fuller
   *   operation string "reading training documents", but it can also contain
   *   a %s, e.g. "reading %s for generating classifier training data", in
   *   which case the string "training documents" will be interpolated at
   *   this point. The reason that a function is returned rather than an
   *   Iterator directly is that some callers may need to process a given
   *   set of raw training documents multiple times.
   */
  def read_raw_training_document_streams:
      Iterable[(String, String => Iterator[DocStatus[Row]])] = {
    (params.input ++ params.train).zipWithIndex.map {
      case (dir, index) => {
        val id = s"#${index + 1}"
        (id, (operation: String) => {
          val task = show_progress(s"$id $operation",
            "training document",
            maxtime = params.max_time_per_stage,
            maxitems = params.num_training_docs)
          val docs = GridDocFactory.read_raw_documents_from_textdb(
            get_file_handler, dir, "-training", with_messages = params.verbose)
          val sleep_at = debugval("sleep-at-docs")
          docs.mapMetered(task) { doc =>
            if (sleep_at != "" && task.num_processed == sleep_at.toInt) {
              errprint("Reached %s documents, sleeping...")
              Thread.sleep(5000)
            }
            doc
          }
        })
      }
    }
  }

  /**
   * Fetch raw training documents as a combined iterator over all corpora.
   *
   * @param operation String identifying the name of the logical operation
   *   that is processing the documents, to be displayed in progress
   *   messages as the documents are read and processed. See
   *   `read_raw_training_document_streams` above.
   * @param streams Set of iterators over raw training documents.
   * @return Iterator over raw training documents.
   */
  def get_combined_raw_training_documents(operation: String,
      streams: Iterable[(String, String => Iterator[DocStatus[Row]])]) =
    streams.map(_._2).toIterator.flatMap(get_docs => get_docs(operation))

  /**
   * Read raw training documents as a combined iterator over all corpora.
   *
   * @param operation String identifying the name of the logical operation
   *   that is processing the documents, to be displayed in progress
   *   messages as the documents are read and processed. See
   *   `read_raw_training_document_streams` above.
   * @return Iterator over raw training documents.
   */
  def read_combined_raw_training_documents(operation: String) =
    get_combined_raw_training_documents(operation,
      read_raw_training_document_streams)

  /**
   * Create a cell grid that's populated with the specified training data.
   * The resulting grid will have a language model (language model)
   * associated with each cell.
   *
   * @param get_rawdocs Function to return an iterator over raw training
   *   documents. This is needed in this form because it may be necessary
   *   to iterate over the documents multiple times. (For example, when
   *   creating a Kd tree, the documents need to be processed in order
   *   to determine the shape of the grid, and then read again to fill in
   *   the grid cells.)
   */
  def create_grid_from_documents(
      supergrid: Option[Grid[Co]],
      level: Int,
      id: String,
      get_rawdocs: String => Iterator[DocStatus[Row]]
  ) = {
    val levelsuff = if (level == 0) "" else s".l$level"
    val grid = supergrid match {
      case None =>
        create_empty_grid(create_document_factory_from_params, id + levelsuff)
      case Some(superg) =>
        superg.create_subdivided_grid(create_document_factory_from_params,
          superg.id + levelsuff)
    }

    // This accesses all the above items, either directly through the variables
    // storing them, or (as for the stopwords and whitelist) through the pointer
    // to this in docfact.
    grid.add_training_documents_to_grid(get_rawdocs)
    if (debug("stop-after-reading-dists")) {
      errprint("Stopping abruptly because debug flag stop-after-reading-dists set")
      output_resource_usage()
      // We throw to top level before exiting because hprof tends to report
      // too much garbage as if it were live.  Unwinding the stack may fix
      // some of that.  If you don't want this unwinding, comment out the
      // throw and uncomment the call to System.exit().
      throw new GridLocateAbruptExit
      // System.exit(0)
    }

    grid.finish_adding_documents()
    if (params.salience_file != null) {
      if (params.verbose)
        errprint("Reading salient points...")
      for (row <- TextDB.read_textdb(get_file_handler, params.salience_file)) {
        val name = row.gets("name")
        val coord = deserialize_coord(row.gets("coord"))
        val salience = row.get[Double]("salience")
        grid.add_salient_point(coord, name, salience)
      }
      if (params.verbose)
        errprint("Reading salient points... done.")
    }
    grid.finish()

    if (params.output_training_cell_lang_models) {
      for (cell <- grid.iter_nonempty_cells) {
        outout(cell.format_location+"\t")
        outprint("%s", cell.grid_lm.debug_string)
      }
    }

    if (debug("output-grid"))
      grid.output_cells(s"$id (level $level)", grid.iter_nonempty_cells)

    grid
  }

  /**
   * Create a cell grid that's populated with training data, as read from
   * the corpus (or corpora) specified in the command-line parameters.
   * If there are multiple input corpora, this will be a combined grid
   * combining sub-grids for each input corpus.
   */
  def create_grid_from_document_streams(
    supergrid: Option[Grid[Co]],
    level: Int,
    streams: Iterable[(String, String => Iterator[DocStatus[Row]])]
  ) = {
    if (streams.size == 1) {
      val (id, get_rawdocs) = streams.head
      create_grid_from_documents(supergrid, level, id, get_rawdocs)
    } else {
      // FIXME! We need to retrieve the component grids from the combined
      // supergrid and create a subgrid from each.
      if (supergrid != None)
        assert(false, "Don't yet know how to handle hierarchical classifier w/multiple grids")
      val grids = streams.map { case (id, get_rawdocs) =>
        create_grid_from_documents(supergrid, level, id, get_rawdocs)
      }
      val combined =
        create_combined_grid(create_document_factory_from_params, "all", grids)
      // We need to do this to get combined backoff stats for the test docs.
      streams.foreach { case (id, get_rawdocs) =>
        combined.add_training_documents_to_grid(get_rawdocs)
      }
      combined.finish_adding_documents()
      combined.finish()
      combined
    }
  }

  def create_combined_grid(docfact: GridDocFactory[Co],
      id: String, grids: Iterable[Grid[Co]]) =
    new InitializedCombinedGrid(docfact, id, grids)

  /**
   * Create a cell grid that's populated with training data, as read from
   * the corpus (or corpora) specified in the command-line parameters.
   * If there are multiple input corpora, this will be a combined grid
   * combining sub-grids for each input corpus.
   */
  def initialize_grid =
    create_grid_from_document_streams(None, 0,
      read_raw_training_document_streams)

  /**
   * Create a ranker object corresponding to the given name. A ranker object
   * returns a ranking over potential grid cells, given a test document.
   * This is used to locate a test document in the grid (e.g. for
   * geolocation), generally by comparing the test document's language model
   * to the language model of each grid cell.
   */
  def create_named_ranker(ranker_name: String, grid: Grid[Co]) = {
    ranker_name match {
      case "random" =>
        new RandomGridRanker[Co](ranker_name, grid)
      case "salience" =>
        new MostPopularGridRanker[Co](ranker_name, grid, salience = true)
      case "num-documents" =>
        new MostPopularGridRanker[Co](ranker_name, grid, salience = false)
      case "naive-bayes" => {
        val features =
          create_naive_bayes_features(params.naive_bayes_feature_list)
        new NaiveBayesGridRanker[Co](ranker_name, grid, features)
      }
      case "classifier" => create_classifier_ranker(ranker_name, grid,
        sort_grid_cells(grid.iter_nonempty_cells), Iterable(), 1, "0")
      case "hierarchical-classifier" => ??? // Should have been handled above
      case "partial-cosine-similarity" =>
        new CosineSimilarityGridRanker[Co](ranker_name, grid, smoothed = false,
          partial = true)
      case "full-cosine-similarity" =>
        new CosineSimilarityGridRanker[Co](ranker_name, grid, smoothed = false,
          partial = false)
      case "smoothed-partial-cosine-similarity" =>
        new CosineSimilarityGridRanker[Co](ranker_name, grid, smoothed = true,
          partial = true)
      case "smoothed-full-cosine-similarity" =>
        new CosineSimilarityGridRanker[Co](ranker_name, grid, smoothed = true,
          partial = false)
      case "partial-kl-divergence" =>
        new KLDivergenceGridRanker[Co](ranker_name, grid, symmetric = false,
          partial = true)
      case "full-kl-divergence" =>
        new KLDivergenceGridRanker[Co](ranker_name, grid, symmetric = false,
          partial = false)
      case "symmetric-partial-kl-divergence" =>
        new KLDivergenceGridRanker[Co](ranker_name, grid, symmetric = true,
          partial = true)
      case "symmetric-full-kl-divergence" =>
        new KLDivergenceGridRanker[Co](ranker_name, grid, symmetric = true,
          partial = false)
      case "sum-frequency" =>
        new SumFrequencyGridRanker[Co](ranker_name, grid)
      case "average-cell-probability" =>
        new AverageCellProbabilityGridRanker[Co](ranker_name, grid)
    }
  }

  protected def create_rough_ranker(args: Array[String]
    ): PointwiseScoreGridRanker[Co]

  /**
   * Create Naive Bayes feature objects, which generate feature probabilities
   * for Naive Bayes, similar to candidate feature-vector factories for the
   * feature-vector-based rankers/classifiers.
   */
  protected def create_naive_bayes_features(feature_list: Iterable[String]) = {
    feature_list map {
      case "terms" => new NaiveBayesTermsFeature[Co]
      case "rough-ranker" => {
        val rough_args = split_command_line(params.rough_ranker_args)
        val ranker = create_rough_ranker(rough_args)
        new NaiveBayesRoughRankerFeature[Co](ranker)
      }
    }
  }

  /**
   * Create a factory object that will train a scoring classifier
   * given appropriate training data. The resulting classifier is
   * typically a linear classifier, which returns a score for a given
   * feature vector by taking a dot product of the feature vector with
   * some learned weights. This is used during reranking: The feature
   * vector describes the compatibility of a test document (a "query")
   * with a given cell (a "candidate"), where the candidate cells are
   * the top N cells taken from some initial ranking (as determined
   * using a ranker object -- see create_ranker). The classifier
   * is trained using a single-weight multi-label training algorithm,
   * where the possible "labels" are in fact a set of candidates for
   * a given training instance, and the algorithm generally tries to
   * find a set of weights that chooses the correct "label" (candidate)
   * for all training documents (i.e. a separating hyperplane).
   * Different algorithms can be used, and the passive-aggressive
   * algorithms attempt to find not only a separating hyperplace but
   * one that maximizes the various scoring margins, for each training
   * document, between the correct candidate for that document and the
   * top-scoring incorrect ones.
   */
  protected def create_pointwise_classifier_trainer[Inst <: GridRankerInst[Co]](
      classifier: String, rerank: Boolean) = {
    val vec_factory = ArrayVector
    def create_weights(weights: VectorAggregate) = {
      if (params.verbose)
        errprint("Creating %s weight vector: length %s",
          params.initial_weights, weights.length)
      params.initial_weights match {
        case "zero" => ()
        case "random" => {
          val r = new scala.util.Random
          for (d <- 0 until weights.depth; i <- 0 until weights.length)
            weights(d)(i) = r.nextGaussian
        }
        case "rank-score-only" => ??? // FIXME!!
      }
      weights
    }
    classifier match {
      case "perceptron" | "avg-perceptron" =>
        new BasicSingleWeightMultiLabelPerceptronTrainer[Inst](
          vec_factory, params.perceptron_aggressiveness,
          decay = params.perceptron_decay,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.iterations,
          averaged = classifier == "avg-perceptron") {
            override def new_weights(len: Int) =
              create_weights(new_zero_weights(len))
          }
      case "pa-perceptron" =>
        new PassiveAggressiveNoCostSingleWeightMultiLabelPerceptronTrainer[Inst](
          vec_factory, params.pa_variant, params.perceptron_aggressiveness,
          decay = params.perceptron_decay,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.iterations) {
            override def new_weights(len: Int) =
              create_weights(new_zero_weights(len))
          }
      case "cost-perceptron" =>
        new PassiveAggressiveCostSensitiveSingleWeightMultiLabelPerceptronTrainer[Inst](
          vec_factory, params.pa_cost_type == "prediction-based",
          params.pa_variant, params.perceptron_aggressiveness,
          decay = params.perceptron_decay,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.iterations) {
            override def new_weights(len: Int) =
              create_weights(new_zero_weights(len))
            def cost(inst: Inst, correct: LabelIndex,
                predicted: LabelIndex) = {
              // Is this checking for correctness itself correct?  Is there a
              // problem with always returning a non-zero cost even when we
              // choose the "correct" cell?  This makes sense in that a
              // candidate is often the "best available" but not necessarily
              // the "best possible".
              if (correct == predicted) 0.0
              else inst.doc.distance_to_cell(inst.get_cell(predicted))
            }
        }
      case "mlogit" =>
        new MLogitConditionalLogitTrainer[Inst](vec_factory)

      case "tadm" =>
        new TADMTrainer[Inst](vec_factory,
          method = params.tadm_method,
          max_iterations = params.iterations,
          gaussian = params.gaussian_penalty,
          lasso = params.lasso_penalty,
          uniform_marginal = params.tadm_uniform_marginal)
    }
  }

  def create_doc_only_fact(
    ty: String,
    featvec_factory: SparseFeatureVectorFactory,
    binning_status: BinningStatus,
    output_skip_message: Boolean
  ): Option[DocFeatVecFactory[Co]] = {
    if (params.features_simple_doc_only_gram_choices.contains(ty))
      Some(new GramDocFeatVecFactory[Co](featvec_factory,
        binning_status, ty))
    else {
      if (output_skip_message)
        errprint(s"Skipping label-dependent feature type $ty")
      None
    }
  }

  def create_doc_cell_fact(
    ty: String,
    featvec_factory: SparseFeatureVectorFactory,
    binning_status: BinningStatus,
    output_skip_message: Boolean
  ): Option[CandidateFeatVecFactory[Co]] = {
    if (params.features_simple_doc_cell_gram_choices contains ty)
      Some(new GramCandidateFeatVecFactory[Co](featvec_factory,
        binning_status, ty))
    else if (params.features_matching_gram_choices contains ty)
      Some(new GramMatchingCandidateFeatVecFactory[Co](featvec_factory,
        binning_status, ty))
    else ty match {
      case "trivial" =>
        Some(new TrivialCandidateFeatVecFactory[Co](featvec_factory,
          binning_status))
      case "misc" =>
        Some(new MiscCandidateFeatVecFactory[Co](featvec_factory,
          binning_status))
      case "types-in-common" =>
        Some(new TypesInCommonCandidateFeatVecFactory[Co](featvec_factory,
          binning_status))
      case "model-compare" =>
        Some(new ModelCompareCandidateFeatVecFactory[Co](featvec_factory,
          binning_status))
      case "rank-score" =>
        Some(new RankScoreCandidateFeatVecFactory[Co](featvec_factory,
          binning_status))
      case _ => {
        if (output_skip_message)
          errprint(s"Skipping document-only feature type $ty")
        None
      }
    }
  }

  /**
   * Used in conjunction with the classifier and reranker. Create a factory
   * that constructs candidate feature vectors for the reranker,
   * i.e. feature vectors measuring the compatibility between a given
   * document and a given cell.
   *
   * @see CandidateFeatVecFactory
   */
  protected def create_combined_featvec_factory(
      feature_list: Iterable[String], binning: String,
      arg: String, attach_label: Boolean,
      include_doc_only: Boolean, include_doc_cell: Boolean,
      output_skip_message: Boolean = false) = {
    require(include_doc_only || include_doc_cell)
    val featvec_factory =
      new SparseFeatureVectorFactory(attach_label = attach_label)
    val binning_status = binning match {
      case "also" => BinningAlso
      case "only" => BinningOnly
      case "no" => BinningNo
    }

    if (feature_list.size == 0)
      params.parser.usageError(s"Can't specify empty --$arg")
    //else if (feature_list.size == 1)
    //  create_fact(feature_list.head)
    else if (!include_doc_cell) {
      val facts = feature_list.flatMap { ty =>
        create_doc_only_fact(ty, featvec_factory, binning_status,
          output_skip_message)
      }
      new CombiningDocFeatVecFactory[Co](featvec_factory, binning_status,
        facts)
    } else {
      val doc_only_facts =
        if (include_doc_only) feature_list.flatMap { ty =>
          create_doc_only_fact(ty, featvec_factory, binning_status,
            output_skip_message)
        }
        else Iterable()
      val facts = doc_only_facts ++ feature_list.flatMap { ty =>
        create_doc_cell_fact(ty, featvec_factory, binning_status,
          output_skip_message)
      }
      new CombiningCandidateFeatVecFactory[Co](featvec_factory, binning_status,
        facts)
    }
  }

  protected def create_classify_featvec_factory(attach_label: Boolean,
      include_doc_only: Boolean, include_doc_cell: Boolean,
      output_skip_message: Boolean = false) = {
    // Factory object for generating feature vectors for a given document,
    // when a given candidate cell is considered as the document's label.
    create_combined_featvec_factory(params.classify_feature_list,
      params.classify_binning, "classify-features",
      attach_label = attach_label, include_doc_only = include_doc_only,
      include_doc_cell = include_doc_cell,
      output_skip_message = output_skip_message)
  }

  protected def create_classify_doc_featvec_factory(attach_label: Boolean,
      output_skip_message: Boolean = false) = {
    create_classify_featvec_factory(attach_label,
        include_doc_only = true, include_doc_cell = false,
        output_skip_message = output_skip_message) match {
      case e:CombiningDocFeatVecFactory[Co] => e
    }
  }

  /**
   * Create a ranker object for ranking test documents. This is currently
   * the top-level entry point for training a model based on training
   * documents.
   */
  def create_ranker: GridRanker[Co] = {
    /* The basic ranker object. */
    def basic_ranker =
      create_ranker_from_document_streams(read_raw_training_document_streams)
    if (params.reranker == "none") basic_ranker
    else if (params.reranker == "random") {
      val reranker = new RandomReranker(basic_ranker, params.rerank_top_n)
      new GridReranker[Co](reranker, params.reranker)
    }
    else
      // FIXME!! We don't use `basic_ranker`. Do we need to create it at all?
      create_classifier_reranker
  }

  def vw_param_args = {
    val gaussian = params.gaussian_penalty
    val lasso = params.lasso_penalty
    (if (gaussian > 0) Seq("--l2", s"$gaussian") else Seq()) ++
    (if (lasso > 0) Seq("--l1", s"$lasso") else Seq()) ++
    Seq("--loss_function", params.vw_loss_function)
  }

  def split_vw_args(vw_args: String) =
    // Split on an empty string wrongly returns Array("")
    (if (vw_args == "") Seq() else vw_args.split("""\s+""").toSeq)

  /**
   * Create a reranker that uses a classifier to do its work.
   */
  def create_vowpal_wabbit_classifier_reranker(load_vw_model: String
      ): GridRanker[Co] = {
    /* Factory object for generating candidate feature vectors
     * to be ranked. There is one such feature vector per cell to be
     * ranked for a given document; many of the features describe
     * the compatibility between the document and cell, e.g. of their
     * language models.
     */
    val candidate_featvec_factory =
      create_combined_featvec_factory(params.rerank_feature_list,
        params.rerank_binning, "rerank-features", attach_label = false,
        include_doc_only = true, include_doc_cell = true)

    val vw_daemon_trainer =
      new VowpalWabbitDaemonTrainer

    if (load_vw_model != "") {
      assert(false, "--load-vw-model not yet implemented for reranker")
      // vw_daemon_trainer.load_vw_model(load_vw_model)
    }

    /* Object for training a reranker. */
    val reranker_trainer =
      new VowpalWabbitGridRerankerTrainer[Co](
        params.reranker, vw_daemon_trainer, params.save_vw_model,
        split_vw_args(params.vw_args) ++ vw_param_args
      ) {
        val top_n = params.rerank_top_n
        val number_of_splits = params.rerank_num_training_splits

        val mapper =
          candidate_featvec_factory.featvec_factory.mapper
        assert_==(mapper.number_of_labels, 0, "#labels")
        for (i <- 0 until params.rerank_top_n) {
          mapper.label_to_index(s"#${i + 1}")
        }

        /* Create the candidate feature vector during training or evaluation,
         * by invoking the candidate feature vector factory (see above).
         */
        def imp_create_candidate_featvec(query: GridDoc[Co],
            candidate: GridCell[Co], initial_score: Double,
            initial_rank: Int, prefix: String) = {
          val featvec_values =
            candidate_featvec_factory.get_features(query, candidate,
              initial_score, initial_rank)
          val featvec = RawSimpleFeatureVector(featvec_values)
          if (debug("features")) {
            errprint("%s: For query %s, candidate %s, initial score %s, initial rank %s, featvec %s",
              prefix, query, candidate, initial_score,
              initial_rank, featvec.pretty_format(prefix))
          }
          featvec
        }

        protected def query_training_data_to_feature_vectors(
          data: Iterable[QueryTrainingInst]
        ): Iterable[(RawAggregateFeatureVector, LabelIndex)] = {

          val task = show_progress("converting QTI's to",
            "aggregate feature vector")

          def create_candidate_featvec(query: GridDoc[Co],
              candidate: GridCell[Co], initial_score: Double,
              initial_rank: Int) = {
            val prefix = "#%s-%s: Training" format (task.num_processed + 1,
              initial_rank)
            imp_create_candidate_featvec(query, candidate, initial_score,
              initial_rank, prefix)
          }

          val maybepar_data = data
          // FIXME: This does not yet work because memoization isn't
          // thread-safe. See the comments for FeatureLabelMapper.
          /*
          val maybepar_data =
            if (!debug("parallel-classifier-training")
                //params.no_parallel || debug("no-parallel-classifier-training")
               )
              data
            else data.par
          */

          maybepar_data.mapMetered(task) { qti =>
            val agg_fv = qti.aggregate_featvec(create_candidate_featvec)
            val label = qti.label
            (agg_fv, label)
          }.seq
        }

        // Create the candidate feature vector during evaluation.
        protected def create_candidate_eval_featvec(query: GridDoc[Co],
            candidate: GridCell[Co], initial_score: Double,
            initial_rank: Int) =
          imp_create_candidate_featvec(query, candidate, initial_score,
            initial_rank, "Eval")

        def create_aggregate_feature_vector(fvs: Iterable[RawFeatureVector]) =
          RawAggregateFeatureVector(fvs.map { rfv =>
            rfv.asInstanceOf[RawSimpleFeatureVector].data })

        def create_training_data(
          data: Iterable[(RawAggregateFeatureVector, LabelIndex)]
        ) = RawAggregateTrainingData(data)

        /* Create the initial ranker from training data. */
        protected def create_initial_ranker(
          data: Iterable[DocStatus[Row]]
        ) = create_ranker_from_document_streams(Iterable(("rerank",
          _ => data.toIterator)))

        /* Convert encapsulated raw documents into document-cell pairs.
         */
        protected def external_instances_to_query_candidate_pairs(
          insts: Iterator[DocStatus[Row]],
          initial_ranker: Ranker[GridDoc[Co], GridCell[Co]]
        ) = {
          val grid_ranker = initial_ranker.asInstanceOf[GridRanker[Co]]
          get_docs_cells_from_raw_documents(grid_ranker.grid, insts)
        }
      }

    /* Training data, in the form of an iterable over raw documents (suitably
     * wrapped in a DocStatus object). */
    val training_data = new Iterable[DocStatus[Row]] {
      def iterator =
        read_combined_raw_training_documents(
          "reading %s for generating reranker training data")
    }.view
    /* Train the reranker. */
    reranker_trainer(training_data)
  }

  /**
   * Create a reranker that uses a classifier to do its work, other
   * than VowpalWabbit.
   */
  def create_non_vw_classifier_reranker: GridRanker[Co] = {
    /* Factory object for generating candidate feature vectors
     * to be ranked. There is one such feature vector per cell to be
     * ranked for a given document; many of the features describe
     * the compatibility between the document and cell, e.g. of their
     * language models.
     */
    val candidate_featvec_factory =
      create_combined_featvec_factory(params.rerank_feature_list,
        params.rerank_binning, "rerank-features", attach_label = false,
        include_doc_only = true, include_doc_cell = true)

    /* Object for training a reranker. */
    val reranker_trainer =
      new LinearClassifierGridRerankerTrainer[Co](
        params.reranker,
        create_pointwise_classifier_trainer[GridRerankerInst[Co]](
          params.reranker, rerank = true)
      ) {
        val top_n = params.rerank_top_n
        val number_of_splits = params.rerank_num_training_splits

        val mapper =
          candidate_featvec_factory.featvec_factory.mapper
        assert_==(mapper.number_of_labels, 0, "#labels")
        for (i <- 0 until params.rerank_top_n) {
          mapper.label_to_index(s"#${i + 1}")
        }

        /* Create the candidate feature vector during training or evaluation,
         * by invoking the candidate feature vector factory (see above).
         * This may need to operate differently during training and
         * evaluation, in particular in that new features cannot be created
         * during evaluation. Hence, e.g., when handling a previously unseen
         * word we need to skip it rather than creating a previously unseen
         * feature. The reason for this is that creating a new feature would
         * increase the total size of the feature vectors and weight vector(s),
         * which we can't do after training has completed. Typically
         * the difference down to 'to_index()' vs. 'to_index_if()'
         * when memoizing.
         */
        def imp_create_candidate_featvec(query: GridDoc[Co],
            candidate: GridCell[Co], initial_score: Double,
            initial_rank: Int, is_training: Boolean,
            prefix: String) = {
          val featvec =
            candidate_featvec_factory(query, candidate, initial_score,
              initial_rank, is_training = is_training)
          if (debug("features")) {
            errprint("%s: For query %s, candidate %s, initial score %s, initial rank %s, featvec %s",
              prefix, query, candidate, initial_score,
              initial_rank, featvec.pretty_format(prefix))
          }
          featvec
        }

        protected def query_training_data_to_feature_vectors(
          data: Iterable[QueryTrainingInst]
        ): Iterable[(GridRerankerInst[Co], LabelIndex)] = {

          val task = show_progress("converting QTI's to",
            "aggregate feature vector")

          def create_candidate_featvec(query: GridDoc[Co],
              candidate: GridCell[Co], initial_score: Double,
              initial_rank: Int) = {
            val prefix = "#%s-%s: Training" format (task.num_processed + 1,
              initial_rank)
            imp_create_candidate_featvec(query, candidate, initial_score,
              initial_rank, is_training = true, prefix)
          }

          val maybepar_data = data
          // FIXME: This does not yet work because memoization isn't
          // thread-safe. See the comments for FeatureLabelMapper.
          /*
          val maybepar_data =
            if (!debug("parallel-classifier-training")
                //params.no_parallel || debug("no-parallel-classifier-training")
               )
              data
            else data.par
          */

          maybepar_data.mapMetered(task) { qti =>
            val agg_fv = qti.aggregate_featvec(create_candidate_featvec)
            val label = qti.label
            val candidates = qti.cand_scores.map(_._1).toIndexedSeq
            (GridRerankerInst(qti.query, agg_fv, candidates), label)
          }.seq
        }

        // Create the candidate feature vector during evaluation.
        protected def create_candidate_eval_featvec(query: GridDoc[Co],
            candidate: GridCell[Co], initial_score: Double,
            initial_rank: Int) =
          imp_create_candidate_featvec(query, candidate, initial_score,
            initial_rank, is_training = false, "Eval")

        def create_aggregate_feature_vector(fvs: Iterable[FeatureVector]) =
          new AggregateFeatureVector(fvs.toArray)

        def create_training_data(
          data: Iterable[(GridRerankerInst[Co], LabelIndex)]
        ) = {
          val result = TrainingData(data)
          val training_data_file = debugval("write-rerank-training-data")
          if (training_data_file != "")
            result.export_to_file(training_data_file)
          result
        }

        /* Create the initial ranker from training data. */
        protected def create_initial_ranker(
          data: Iterable[DocStatus[Row]]
        ) = create_ranker_from_document_streams(Iterable(("rerank",
          _ => data.toIterator)))

        /* Convert encapsulated raw documents into document-cell pairs.
         */
        protected def external_instances_to_query_candidate_pairs(
          insts: Iterator[DocStatus[Row]],
          initial_ranker: Ranker[GridDoc[Co], GridCell[Co]]
        ) = {
          val grid_ranker = initial_ranker.asInstanceOf[GridRanker[Co]]
          get_docs_cells_from_raw_documents(grid_ranker.grid, insts)
        }
      }

    /* Training data, in the form of an iterable over raw documents (suitably
     * wrapped in a DocStatus object). */
    val training_data = new Iterable[DocStatus[Row]] {
      def iterator =
        read_combined_raw_training_documents(
          "reading %s for generating reranker training data")
    }.view
    /* Train the reranker. */
    reranker_trainer(training_data)
  }

  /**
   * Create a reranker that uses a classifier to do its work.
   */
  def create_classifier_reranker: GridRanker[Co] = {
    if (params.reranker == "vowpal-wabbit")
      create_vowpal_wabbit_classifier_reranker(params.load_vw_model)
    else
      create_non_vw_classifier_reranker
  }

  // Convert documents into document-cell pairs, for the correct cell.
  def get_docs_cells_from_documents(grid: Grid[Co],
      docs: Iterator[GridDoc[Co]]) = {
    docs flatMap {
      doc =>
        // Convert document to (doc, cell) pair.  But if a cell
        // can't be found (i.e. there were no training docs in the
        // cell of this "test" doc), skip the entire instance rather
        // than end up trying to score a fake cell. (FIXME: Does this
        // ever happen if we're reading training documents? Presumably not?)
        grid.find_best_cell_for_document(doc,
          create_non_recorded = false) map ((doc, _))
    }
  }

  // Convert raw documents into document-cell pairs, for the correct cell.
  def get_docs_cells_from_raw_documents(grid: Grid[Co],
      raw_docs: Iterator[DocStatus[Row]]) = {
    get_docs_cells_from_documents(grid,
      grid.docfact.raw_documents_to_documents(raw_docs))
  }

  // Fetch document-cell pairs, for the correct cell, from training data.
  // The document-cell pairs can either be passed in already (if they are
  // cached for speed) or read from external training data.
  def get_docs_cells(grid: Grid[Co],
      xdocs_cells: Iterable[(GridDoc[Co], GridCell[Co])]) = {
    if (xdocs_cells.size == 0) {
      val raw_training_docs = read_combined_raw_training_documents(
         "reading %s for generating classifier training data")
      get_docs_cells_from_raw_documents(grid, raw_training_docs)
    } else {
      xdocs_cells.toIterator
    }
  }

  // Fetch document-cell pairs, for the correct cell, from training data.
  // The document-cell pairs can either be passed in already (if they are
  // cached for speed) or read from external training data. Filter to match
  // only documents whose correct cell is one of the specified candidates.
  def get_filtered_docs_cells(grid: Grid[Co],
      candidates: Iterable[GridCell[Co]],
      xdocs_cells: Iterable[(GridDoc[Co], GridCell[Co])]) = {
    val docs_cells = get_docs_cells(grid, xdocs_cells)
    val cand_set = candidates.toSet
    docs_cells.filter { case (doc, cell) =>
      cand_set contains cell
    }
  }

  // Sort grid cells in a consistent fashion.
  def sort_grid_cells(cells: Iterable[GridCell[Co]]) = cells.toIndexedSeq.sorted

  /**
   * Create a ranker that uses a classifier to do its work, treating each
   * cell as a possible class.
   *
   * @param ranker_name An identifying string, usually "classifier".
   * @param grid Grid containing cells.
   * @param candidates Candidate cells (aka classes, labels) that the
   *   classifier chooses among. MUST BE SORTED. There may be fewer of them
   *   than total cells in the grid. They should normally belong to the grid
   *   specified above, but there may be exceptions (e.g. with K-d trees,
   *   where levels &gt; 1 have grids that share the cells with the grid at
   *   level 1, hence the cell's grid will identify the grid at level 1 not
   *   the sub-level grid).
   * @param xdocs_cells Training documents and corresponding correct cells,
   *   for training the classifier. If empty, read external training documents
   *   according to '--input' and/or positional params.
   * @param level Level of this classifier. Normally 1, but may be &gt; 1
   *   in hierarchical classification.
   * @param cindex Index of this classifier, a combination of a number and
   *   possibly a cell-coordinate identifier (the center). Will be "0" if
   *   if level == 1.
   */
  def create_classifier_ranker(ranker_name: String,
      grid: Grid[Co], candidates: IndexedSeq[GridCell[Co]],
      // IMPORTANT: Don't calculate xdocs_cells till necessary, to avoid
      // reloading the training documents when we are loading a pre-saved
      // model. See comment in create_hierarchical_classifier_ranker.
      xdocs_cells: => Iterable[(GridDoc[Co], GridCell[Co])],
      level: Int, cindex: String) = {
    assert(is_sorted(candidates))
    // FIXME: This doesn't apply for K-d trees under hierarchical
    // classification, where we have a copy of the grid that shares the
    // same cells.
    // candidates.foreach { cand => assert_==(cand.grid, grid, "grid") }
    // xdocs_cells.foreach { case (doc, cell) =>
    //   assert_==(cell.grid, grid, "grid") }
    // Make this lazy so we don't calculate it if not necessary; see comment
    // above.
    lazy val docs_cells =
      get_filtered_docs_cells(grid, candidates, xdocs_cells)
    if (params.classifier == "vowpal-wabbit" ||
        params.classifier == "cost-vowpal-wabbit") {
      val vw_args =
        if (level > 1 && params.nested_vw_args != null) params.nested_vw_args
        else params.vw_args
      val cost_sensitive = params.classifier == "cost-vowpal-wabbit"
      def replace_level_index(str: String) =
        str.replace("%l", level.toString).replace("%i", cindex)
      val save_vw_model =
        if (level > 1) replace_level_index(params.save_vw_submodels)
        else params.save_vw_model
      val load_vw_model =
        if (level > 1) replace_level_index(params.load_vw_submodels)
        else params.load_vw_model
      val (classifier, featvec_factory) =
        create_vowpal_wabbit_classifier(grid, candidates, docs_cells,
          save_vw_model, load_vw_model, vw_args,
          cost_sensitive = cost_sensitive)
      new VowpalWabbitGridRanker[Co](ranker_name, grid, classifier,
        featvec_factory, cost_sensitive = cost_sensitive)
    } else {
      val featvec_factory = create_classify_featvec_factory(attach_label = true,
        include_doc_only = true, include_doc_cell = true)

      /*
       * We need to do the following during training:
       *
       * 1. Go through all the documents and create feature vectors for the
       *    doc-only features, using the feature property indices.
       * 2. Set ND = #doc-only features.
       * 3. Go through the documents again, zipped with the corresponding
       *    feature vectors, and loop through the cells. For each cell,
       *    create a feature vector consisting of the original vector scaled
       *    up appropriately (with ND*cell-id added to each feature ID) +
       *    new doc-cell features created using the original two-level
       *    cell-specific feature memoization technique, with an offset
       *    ND*#cells added to all features set this way.
       *
       * During training time, we do something similar but for each
       * document separately, since ND isn't changing.
       *
       * To do this, perhaps at first only implement doc-only features.
       */
      val task = show_progress("generating", "doc agg feat vec")

      // Generate training data. For each document, iterate over the
      // possible cells, generating a feature vector for each cell.
      // This actually holds an iterator; nothing gets done until we
      // convert to an indexed sequence, below.
      val training_instances =
        docs_cells.mapMetered(task) { case (doc, correct_cell) =>
          // Maybe do it in parallel.
          val maybepar_cells = candidates
          // FIXME: This does not yet work because memoization isn't
          // thread-safe. See the comments for FeatureLabelMapper.
          /*
          val maybepar_cells =
            if (!debug("parallel-classifier-training")
                //params.no_parallel || debug("no-parallel-classifier-training")
               )
              candidates
            else candidates.par
          */

          // Iterate over cells.
          val fvs = maybepar_cells.map { cell =>
            // Create feature vector. We don't have an initial score or
            // initial ranking, so pass in 0. This is only used by the
            // 'rank-score' feature-vector factory.
            val featvec = featvec_factory(doc, cell, 0, 0, is_training = true)
            // Maybe output feature vector, for debugging purposes.
            if (debug("features")) {
              val prefix = "#%s" format (task.num_processed + 1)
              errprint(s"$prefix: Training: For doc $doc, cell $cell, featvec ${featvec.pretty_format(prefix)}")
            }
            featvec
          }

          // Aggregate feature vectors.
          val agg_fv = AggregateFeatureVector(fvs.toArray)
          // Generate data instance.
          val data_inst = GridRankingClassifierInst(doc, agg_fv, featvec_factory)
          // Return it, along with correct cell's label. The correct cell
          // should always have been seen already, since we filtered the
          // instances we process to be those that have the correct cell
          // among the candidates and we already indexed all the candidates
          // using 'lookup_cell'. So make sure we don't accidentally create
          // a new cell index by using 'lookup_cell_if'.
          (data_inst, featvec_factory.lookup_cell_if(correct_cell).get)
        }

      // Create classifier trainer.
      val trainer =
        create_pointwise_classifier_trainer[GridRankingClassifierInst[Co]](
          params.classifier, rerank = false)

      // Train classifier.
      val classifier =
        trainer(TrainingData(training_instances.toIndexedSeq,
          remove_non_choice_specific_columns = false))
      new IndivClassifierGridRanker[Co](ranker_name, grid, classifier,
        featvec_factory)
    }
  }

  def vowpal_wabbit_cost_function(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val dist = doc.distance_to_cell(cell)
    params.vw_cost_function match {
      case "dist" => dist
      case "sqrt-dist" => math.sqrt(dist)
      case "log-dist" => if (dist < 1.0) 0.0 else math.log(dist)
    }
  }

  /**
   * Create a classifer that uses Vowpal Wabbit to do its work,
   * treating each cell in the candidate list as a possible class.
   */
  def create_vowpal_wabbit_classifier(grid: Grid[Co],
      candidates: IndexedSeq[GridCell[Co]], // MUST BE SORTED
      // IMPORTANT: Don't calculate docs_cells till necessary, to avoid
      // reloading the training documents when we are loading a pre-saved
      // model. See comment in create_hierarchical_classifier_ranker.
      docs_cells: => Iterator[(GridDoc[Co], GridCell[Co])],
      save_vw_model: String, load_vw_model: String, vw_args: String,
      cost_sensitive: Boolean) = {
    val featvec_factory =
      create_classify_doc_featvec_factory(attach_label = false,
        output_skip_message = true)

    // Candidate must be sorted so we get a more predictable and
    // consistent mapping between cells and labels.
    assert(is_sorted(candidates))

    // Do this first so there's an entry in the featvec_factory for each
    // non-empty cell, even when loading a model.
    val cells_labels =
      candidates.map { cell =>
        (cell, featvec_factory.lookup_cell(cell))
      }
    assert_==(cells_labels.size,
      featvec_factory.featvec_factory.mapper.number_of_labels)

    val cell_label_mapping_file = debugval("write-cell-label-mapping")
    if (cell_label_mapping_file != "") {
      val rows =
        for ((cell, label) <- cells_labels) yield {
          Seq("label" -> label,
            "cell" -> cell.format_location,
            "cell-centroid" -> cell.format_coord(cell.get_centroid),
            "most-salient-document" ->
              Encoder.string(cell.most_salient_point),
            "most-salient-document-salience" ->
              cell.most_salient_point_salience
          )
        }
      TextDB.write_constructed_textdb(localfh, cell_label_mapping_file,
        rows.iterator)
    }
    // Create classifier trainer.
    val trainer = new VowpalWabbitBatchTrainer

    if (load_vw_model != "")
      (trainer.load_vw_model(load_vw_model), featvec_factory)
    else {
      // val task = show_progress("processing features in", "document")

      // Create the actual classifier, using the given user-supplied arguments
      // (which may come from '--vw-args', '--nested-vw-args' or
      // '--fallback-vw-args'.
      def create_classifier(vw_args: String) = {
        val feats_filename = if (cost_sensitive) {
          val training_data =
            docs_cells.map/*Metered(task)*/ { case (doc, correct_cell) =>
            val feats = featvec_factory.get_features(doc)
            val cells_costs = cells_labels.map { case (cell, label) =>
              (label,
                // Uncomment to always use cost 0.0 for the correct cell
                // instead of actual error distance.  Doesn't seem to make
                // much difference.
                // if (label == correct_cell) 0.0 else
                vowpal_wabbit_cost_function(doc, cell))
            }
            (feats, cells_costs)
          }

          // Train classifier.
          val feats_filename =
            trainer.write_cost_sensitive_feature_file(training_data)
          feats_filename
        } else {
          val training_data =
            docs_cells.map/*Metered(task)*/ { case (doc, correct_cell) =>
            val feats = featvec_factory.get_features(doc)
            // The correct cell should always have been seen already,
            // since we filtered the instances we process to be those that
            // have the correct cell among the candidates and we already
            // indexed all the candidates using 'lookup_cell'. So make sure
            // we don't accidentally create a new cell index by using
            // 'lookup_cell_if'.
            (feats, featvec_factory.lookup_cell_if(correct_cell).get)
          }

          trainer.write_feature_file(training_data)
        }

        // Train classifier.
        val num_labels =
          featvec_factory.featvec_factory.mapper.number_of_labels
        assert_==(cells_labels.size, num_labels,
          "#labels")
        assert_==(candidates.size, num_labels,
          "#labels")
        val extra_args = vw_param_args ++
          (if (cost_sensitive) {
            val multiclass_arg =
              if (params.vw_multiclass == "oaa") "csoaa" else "wap"
            Seq("--" + multiclass_arg, s"$num_labels")
          } else {
            Seq("--" + params.vw_multiclass, s"$num_labels")
          })
        trainer(feats_filename, save_vw_model,
          split_vw_args(vw_args) ++ extra_args)
      }

      val classifier =
        try {
          create_classifier(vw_args)
        } catch {
          case e:VowpalWabbitModelError => {
            errprint(s"Warning, bad model, falling back to args '${params.fallback_vw_args}'")
            create_classifier(params.fallback_vw_args)
          }
        }

      (classifier, featvec_factory)
    }
  }

  /**
   * Create a hierarchical classifier ranker, which classifies at multiple
   * levels, from coarse to fine.
   */
  def create_hierarchical_classifier_ranker(ranker_name: String,
      streams: Iterable[(String, String => Iterator[DocStatus[Row]])]) = {
    val coarse_grid = create_grid_from_document_streams(None, 1, streams)
    // Sort the grid cells so the numbered cells corresponding to different
    // classifiers have a consistent order to make loading/saving of submodels
    // possible, and numbered labels for a given classifier have a consistent
    // order to make loading/saving of a given model possible. (Note that the
    // underlying type of `iter_nonempty_cells` in K-d trees is a Set, which
    // would cause all sorts of order-related problems.)
    val candidates = sort_grid_cells(coarse_grid.iter_nonempty_cells)
    val raw_training_docs =
      get_combined_raw_training_documents(
        "reading %s for hierarchical classifier training data",
        streams)
    // Make this lazy so we don't calculate it if not necessary, to avoid
    // reloading the training documents when we're loading a pre-saved model.
    // Because we pass this to a function, the function needs to declare
    // the corresponding parameter to be "lazy" (aka pass by name, so it
    // gets evaluated only on first use) and variables that make use of
    // this parameter have to be lazy as well. This way, when loading a
    // pre-saved model, where we never use this info, we never end up
    // evaluating this lazy variable or any dependent lazy variable, and
    // as a result we don't reload the whole set of training documents,
    // which can take several minutes.
    lazy val docs_cells = get_docs_cells_from_raw_documents(coarse_grid,
      raw_training_docs).toIndexedSeq
    val coarse_ranker = create_classifier_ranker(ranker_name, coarse_grid,
      candidates, docs_cells, 1, "0")
    val grids = mutable.Buffer[Grid[Co]]()
    grids += coarse_grid
    var lastgrid = coarse_grid
    var lastcands = candidates
    // FIXME! What happens when we are reading multiple streams and creating
    // multiple grids? docs_cells combines all the streams; does this make
    // sense?
    val finer_rankers = for (level <- 2 to params.num_levels) yield {
      val num_cands = lastcands.size
      errprint("Beginning hierarchical level %s: Creating %s classifiers",
        level, num_cands)
      val subgrid =
        create_grid_from_document_streams(Some(lastgrid), level, streams)
      // docs_cells contains cells from the coarsest grid; we need to redo
      // it to contain cells from the subgrid we just created. Make it lazy
      // to avoid reloading the training documents when we're loading a
      // pre-saved model.
      lazy val finer_docs_cells =
        get_docs_cells_from_documents(subgrid, docs_cells.toIterator.map {
          case (doc, cell) => doc }
        ).toIndexedSeq
      // Retrieve a list, for each candidate cell, of a tuple
      // (SUBCANDS, CAND -> SUBRANKER), i.e. list of candidate sub-cells to
      // choose among and a mapping from the candidate to a ranker object to
      // do the choosing.
      val subcands_lists_ranker_entries = {
        val lastcands2 =
          if (debug("parallel-hier-classifier")) lastcands.par
          else lastcands
        lastcands2.zipWithIndex.map { case (cand, index) =>
          errprint("Level %s: Creating classifier %s/%s", level,
            index + 1, num_cands)
          // Need to sort, see above.
          val subcands = sort_grid_cells(subgrid.get_subdivided_cells(cand))
          assert(subcands.size > 0,
            s"Saw zero-length subcells for cell $cand (index ${index + 1}) at centroid ${cand.get_centroid}")
          if (debug("hier-classifier")) {
            errprint("For candidate %s:", cand)
            errprint("#Subdivided cells: %s", subcands.size)
            for ((subcand, index) <- subcands.zipWithIndex) {
              errprint(s"#${index + 1}: ${subcand.format_coord(subcand.get_central_point)}, numdocs = ${subcand.num_docs}")
            }
          }
          val subranker = create_classifier_ranker(ranker_name,
            subgrid, subcands, finer_docs_cells, level,
            s"$index${cand.format_coord(cand.get_true_center)}")
          (subcands, cand -> subranker)
        }.seq
      }
      val (subcands_lists, ranker_entries) =
        subcands_lists_ranker_entries.unzip
      // Get the whole list of candidate cells at the next level, remove
      // any duplicates and sort (see comments above for why we need to do
      // this). FIXME: This list should probably be the same as the list
      // returned by iter_nonempty_cells for the appropriate grid, and we
      // should perhaps add an assertion to check this.
      val subcands = sort_grid_cells(subcands_lists.flatten.toSet)
      val rankers = ranker_entries.toMap
      grids += subgrid
      lastgrid = subgrid
      lastcands = subcands
      rankers
    }

    new HierarchicalClassifierGridRanker[Co](ranker_name, grids,
      coarse_ranker, finer_rankers, // docs_cells,
      params.beam_size)
  }

  /**
   * Create a grid populated from the specified training documents
   * (`create_grid_from_document_streams`), then create a ranker object that
   * references this grid.
   */
  def create_ranker_from_document_streams(
    streams: Iterable[(String, String => Iterator[DocStatus[Row]])]
  ) = {
    if (params.ranker == "hierarchical-classifier")
      create_hierarchical_classifier_ranker(params.ranker, streams)
    else {
      val grid = create_grid_from_document_streams(None, 0, streams)
      create_named_ranker(params.ranker, grid)
    }
  }

  /**
   * Output, to a textdb corpus, the results of locating the best cell
   * for each document in a set of test documents.
   */
  def write_results_file(results: Iterator[DocEvalResult[Co]],
      filehand: FileHandler, base: String) {
    note_result("textdb-type", "textgrounder-results")
    write_constructed_textdb_with_results(filehand, base, results.map(_.to_row))
  }
}

class GridLocateAbruptExit extends Throwable { }

abstract class GridLocateApp(appname: String) extends
    ExperimentDriverApp(appname) {
  type TDriver <: GridLocateDriver[_]

  override def run_program(args: Array[String]) = {
    try {
      super.run_program(args)
    } catch {
      case e:GridLocateAbruptExit => {
        errprint("Caught abrupt exit throw, exiting")
        0
      }
      case e:java.lang.OutOfMemoryError => {
        errprint("Caught java.lang.OutOfMemoryError")
        output_resource_usage()
        throw e
      }
    }
  }
}
