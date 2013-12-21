///////////////////////////////////////////////////////////////////////////////
//  GridLocate.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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
import util.spherical._
import util.experiment._
import util.io.FileHandler
import util.metering._
import util.os.output_resource_usage
import util.print._
import util.textdb._
import util.debug._

import learning._
import learning.perceptron._
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
  val default_gridranksize = 11
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
}

/**
 * Mixin holding basic GridLocate parameters.
 */
trait GridLocateBasicParameters {
  this: GridLocateParameters =>

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
    ap.multiOption[String]("i", "input", "input-corpus",
      metavar = "FILE",
      help = """Input corpus, a textdb database (a type of flat-file
database, with separate schema and data files). The value can be any of
the following: Either the data or schema file of the database; the common
prefix of the two; or the directory containing them, provided there is
only one textdb in the directory.

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
Wikipedia article, etc.).

Multiple such files can be given by specifying the option multiple
times.""")

  if (ap.parsedValues) {
    if (input.length == 0)
      ap.error("Must specify input file(s) using --input")
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
    ap.option[Int]("raw-text-max-ngram", "rdmn", metavar = "NUM",
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

  //// Options used when doing Naive Bayes geolocation
  var naive_bayes_weighting =
    ap.option[String]("naive-bayes-weighting", "nbw", metavar = "STRATEGY",
      default = "equal",
      choices = Seq("equal", "equal-words", "distance-weighted"),
      help = """Strategy for weighting the different probabilities
that go into Naive Bayes.  If 'equal', do pure Naive Bayes, weighting the
prior probability (baseline) and all word probabilities the same.  If
'equal-words', weight all the words the same but collectively weight all words
against the baseline, giving the baseline weight according to --baseline-weight
and assigning the remainder to the words.  If 'distance-weighted' (NOT
currently implemented), similar to 'equal-words' but don't weight each word
the same as each other word; instead, weight the words according to distance
from the toponym.""")
  var naive_bayes_baseline_weight =
    ap.option[Double]("naive-bayes-baseline-weight", "nbbw",
      metavar = "WEIGHT",
      default = 0.5,
      must = be_>=(0.0),
      help = """Relative weight to assign to the baseline (prior
probability) when doing weighted Naive Bayes.  Default %default.""")

  //// Options used when doing ACP geolocation
  var lru_cache_size =
    ap.option[Int]("lru-cache-size", "lru", metavar = "SIZE",
      default = 400,
      must = be_>(0),
      help = """Number of entries in the LRU cache.  Default %default.
Used only when --ranker=average-cell-probability.""")

  var stopwords_file =
    ap.option[String]("stopwords-file",
      metavar = "FILE",
      help = """File containing list of stopwords.  If not specified,
a default list of English stopwords (stored in the TextGrounder distribution)
is used.""")

  var whitelist_file =
    ap.option[String]("whitelist-file",
       metavar = "FILE",
       help = """File containing a whitelist of words. If specified, ONLY
words on the list will be read from any corpora; other words will be ignored.
If not specified, all words (except those on the stopword list) will be
read.""")

  var weights_file =
    ap.option[String]("weights-file",
       metavar = "FILE",
       help = """File containing a set of word weights for weighting words
non-uniformly (e.g. so that more geographically informative words are weighted
higher). The file should be in textdb format, with `word` and `weight` fields.
The weights do not need to be normalized but must not be negative. They will
be normalized so that the average is 1. Each time a word is read in, it will
be given a count corresponding to its normalized weight rather than a count
of 1.

The parameter value can be any of the following: Either the data or schema
file of the database; the common prefix of the two; or the directory containing
them, provided there is only one textdb in the directory.""")

  var missing_word_weight =
    ap.option[Double]("missing-word-weight", "mww",
       metavar = "WEIGHT",
       default = -1.0,
       help = """Weight to use for words not given in the word-weight
file, when `--weights-file` is given. If negative (the default), use
the average of all weights in the file, which treats them as if they
have no special weight. It is possible to set this value to zero, in
which case unspecified words will be ignored.""")

  var ignore_weights_below =
    ap.option[Double]("ignore-weights-below", "iwb",
       metavar = "WEIGHT",
       help = """If given, ignore words whose weights are below the given
value. If you specify this, you might also want to specify
`--missing-word-weight 0` so that words with a missing or ignored weight are
assigned a weight of 0 rather than the average of the weights that have been
seen.""")

  var output_training_cell_lang_models =
    ap.flag("output-training-cell-lang-models", "output-training-cells",
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
gazetteer with their population used as the salience value. For each grid
cell, the most salient item in the cell will be noted, and when a particular
grid cell is identified in diagnostic output (e.g. in `--print-results`), the
salient item will be displayed along with the cell's coordinates to make it
easier to identify the nature of the cell in question.

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
      aliasedChoices = Seq(Seq("dev", "devel"), Seq("test")),
      help = """Set to use for evaluation during document geolocation when
when --eval-format=internal ('dev' or 'devel' for the development set,
'test' for the test set).  Default '%default'.""")

  var eval_file =
    ap.multiOption[String]("e", "eval-file",
      metavar = "FILE",
      help = """File or directory containing files to evaluate on.
Multiple such files/directories can be given by specifying the option multiple
times.  If a directory is given, all files in the directory will be
considered (but if an error occurs upon parsing a file, it will be ignored).
Each file is read in and then disambiguation is performed.  Not used during
document geolocation when --eval-format=internal (the default).""")

  //// Eval output options
  var results =
    ap.option[String]("r", "results",
      metavar = "FILE",
      help = """If specified, prefix of file to store results into.
Results are also normally output to stderr for debugging purposes unless
`--no-results` is given.  Results are stored as a textdb database, i.e. two
files will be written, with extensions `.data.txt` and `.schema.txt`, with
the former storing the data as tab-separated fields and the latter naming
the fields.""")

  var print_results =
    ap.flag("print-results", "show-results",
      help = """Show individual results for each test document.""")

  var results_by_range =
    ap.flag("results-by-range",
      help = """Show results by range (of error distances and number of
documents in correct cell).  Not on by default as counters are used for this,
and setting so many counters breaks some Hadoop installations.""")

  var num_nearest_neighbors =
    ap.option[Int]("num-nearest-neighbors", "knn", default = 4,
      must = be_>(0),
      help = """Number of nearest neighbors (k in kNN); default is %default.""")

  var num_top_cells_to_output =
    ap.option[Int]("num-top-cells-to-output", "num-top-cells", default = 5,
      help = """Number of nearest neighbor cells to output; default is %default;
-1 means output all""")

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

  protected def ranker_default = "naive-bayes-no-baseline"
  protected def ranker_choices = Seq(
        Seq("full-kl-divergence", "full-kldiv", "full-kl"),
        Seq("partial-kl-divergence", "partial-kldiv", "partial-kl", "part-kl"),
        Seq("symmetric-full-kl-divergence", "symmetric-full-kldiv",
            "symmetric-full-kl", "sym-full-kl"),
        Seq("symmetric-partial-kl-divergence",
            "symmetric-partial-kldiv", "symmetric-partial-kl", "sym-part-kl"),
        Seq("cosine-similarity", "cossim"),
        Seq("partial-cosine-similarity", "partial-cossim", "part-cossim"),
        Seq("smoothed-cosine-similarity", "smoothed-cossim"),
        Seq("smoothed-partial-cosine-similarity", "smoothed-partial-cossim",
            "smoothed-part-cossim"),
        Seq("naive-bayes-with-baseline", "nb-base"),
        Seq("naive-bayes-no-baseline", "nb-nobase"),
        Seq("average-cell-probability", "avg-cell-prob", "acp"),
        Seq("salience", "internal-link"),
        Seq("random"),
        Seq("num-documents", "numdocs", "num-docs"))

  protected def ranker_non_baseline_help =
"""'full-kl-divergence' (or 'full-kldiv') searches for the cell where the KL
divergence between the document and cell is smallest.

'partial-kl-divergence' (or 'partial-kldiv') is similar but uses an
abbreviated KL divergence measure that only considers the words seen in the
document; empirically, this appears to work just as well as the full KL
divergence.

'naive-bayes-with-baseline' and 'naive-bayes-no-baseline' use the Naive
Bayes algorithm to match a test document against a training document (e.g.
by assuming that the words of the test document are independent of each
other, if we are using a unigram language model).  The variants with
the "baseline" incorporate a prior probability into the calculations, while
the non-baseline variants don't.  The baseline is currently derived from the
number of documents in a cell.  See also 'naive-bayes-weighting' and
'naive-bayes-baseline-weight' for options controlling how the different
words are weighted against each other and how the baseline and word
probabilities are weighted.

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
    ap.option[String]("s", "ranker", "strategy",
      default = ranker_default,
      aliasedChoices = ranker_choices,
      help = """Ranking strategy/strategies to use for geolocation.
""" + ranker_non_baseline_help +
"""In addition, the following "baseline" probabilities exist, which use
simple algorithms meant for comparison purposes.

""" + ranker_baseline_help +
"""Default is '%default'.""")
}

trait GridLocateRerankParameters {
  this: GridLocateParameters =>

  var rerank =
    ap.flag("rerank",
      help = """If specified, do reranking of grid cells.""")

  var rerank_classifier =
    ap.option[String]("rerank-optimizer",
      default = "avg-perceptron",
      choices = Seq("perceptron", "avg-perceptron", "pa-perceptron",
        "cost-perceptron", "mlogit"),
      help = """Type of optimizer to use for reranking.  Possibilities are:

'perceptron' (perceptron using the basic algorithm);

'avg-perceptron' (perceptron using the basic algorithm, where the weights
  from the various rounds are averaged -- this usually improves results if
  the weights oscillate around a certain error rate, rather than steadily
  improving);

'pa-perceptron' (passive-aggressive perceptron, which usually leads to steady
  but gradually dropping-off error rate improvements with increased number
  of rounds);

'cost-perceptron' (cost-sensitive passive-aggressive perceptron, using the
  error distance as the cost);

'mlogit' (use a generalized linear model, specifically a conditional logit
  model -- similar to a multinomial logistic regression or maximum entropy
  model).

Default %default.

For the perceptron optimizers, see also `--pa-variant`,
`--perceptron-error-threshold`, `--perceptron-aggressiveness` and
`--perceptron-rounds`.""")

  protected def with_binned(feats: String*) =
    feats flatMap { f => Seq(f, f + "-binned") }

  val rerank_features_simple_unigram_choices =
    (Seq("binary") ++
      with_binned("doc-count", "doc-prob", "cell-count", "cell-prob")
    ).map { "unigram-" + _ }

  val rerank_features_matching_unigram_choices =
    (Seq("binary") ++ with_binned(
      "doc-count", "cell-count", "doc-prob", "cell-prob",
      "count-product", "count-quotient", "prob-product", "prob-quotient",
      "kl")
    ).map { "matching-unigram-" + _ }

  val rerank_features_simple_ngram_choices =
    (Seq("binary") ++
      with_binned("doc-count", "doc-prob", "cell-count", "cell-prob")
    ).map { "ngram-" + _ }

  val rerank_features_matching_ngram_choices =
    (Seq("binary") ++ with_binned(
      "doc-count", "cell-count", "doc-prob", "cell-prob",
      "count-product", "count-quotient", "prob-product", "prob-quotient",
      "kl")
    ).map { "matching-ngram-" + _ }

  val rerank_features_all_unigram_choices =
    rerank_features_simple_unigram_choices ++
    rerank_features_matching_unigram_choices

  val rerank_features_all_ngram_choices =
    rerank_features_simple_ngram_choices ++
    rerank_features_matching_ngram_choices

  val allowed_rerank_features =
    Seq("misc", "types-in-common", "model-compare", "rank-score", "trivial") ++
    rerank_features_all_unigram_choices ++
    rerank_features_all_ngram_choices

  var rerank_features =
    ap.option[String]("rerank-features",
      default = "misc,rank-score",
      help = """Which features to use in the reranker, to characterize the
similarity between a document and candidate cell (largely based on the
respective language models). Possibilities are:

'trivial' (no features, for testing purposes);

'rank-score' (use the original rank and ranking score);

'unigram-binary' (use the value 1 when a word exists in the document,
  0 otherwise);

'unigram-doc-count' (use the document word count when a word exists in
  the document);

'unigram-doc-prob' (use the document word probability when a word exists in
  the document);

'unigram-cell-count' (use the cell word count when a word exists in
  the document);

'unigram-cell-prob' (use the cell word probability when a word exists in
  the document);

'matching-unigram-binary' (use the value 1 when a word exists in both document
  and cell, 0 otherwise);

'matching-unigram-doc-count' (use the document word count when a word exists
  in both document and cell);

'matching-unigram-doc-prob' (use the document word probability when a word
  exists in both document and cell);

'matching-unigram-cell-count' (use the cell word count when a word exists
  in both document and cell -- equivalent to 'unigram-cell-count');

'matching-unigram-cell-prob' (use the cell word probability when a word exists
  in both document and cell);

'matching-unigram-count-product' (use the product of the document and cell
  word count when a word exists in both document and cell);

'matching-unigram-count-quotient' (use the quotient of the cell and document
  word count when a word exists in both document and cell);

'matching-unigram-prob-product' (use the product of both the document and cell
  probability when a word exists in both document and cell);

'matching-unigram-prob-quotient' (use the quotient of the cell and document
  word probability when a word exists in both document and cell);

'matching-unigram-kl' (when a word exists in both document and cell, use the
  individual KL-divergence component score between document and cell for the
  word, else 0);

'ngram-*', 'matching-ngram-*' (similar to the corresponding unigram features
  but include features for N-grams up to --max-rerank-ngram);

'*-binned' (for all feature types given above except for the '*-binary'
  types, a "binned" equivalent exists that creates binary features for
  different ranges (aka bins) of values, generally logarithmically, but
  by fixed increments for fractions and similarly bounded, additive values

'types-in-common' (number of word/ngram types in the document that are also
  found in the cell)

'model-compare' (various ways of comparing the language models of document and
  cell: KL-divergence, symmetric KL-divergence, cosine similarity, Naive Bayes)

'misc' (other non-word-specific features, e.g. number of documents in a cell,
  number of word types/tokens in a document/cell, etc.; should be fairly fast
  to compute)

Multiple feature types can be specified, separated by spaces or commas.

Default %default.

Note that this only is used when --rerank=pointwise and --rerank-classifier
specifies something other than 'trivial'.""")

  lazy val rerank_feature_list = {
    val features = rerank_features.split("[ ,]")
    for (feature <- features) {
      if (!(allowed_rerank_features contains feature))
        ap.usageError("Unrecognized feature '%s' in --rerank-features" format
          feature)
    }
    features.toSeq
  }

  var rerank_binning =
    ap.option[String]("rerank-binning", "rb", "bin", metavar = "BINNING",
      default = "also",
      aliasedChoices = Seq(
        Seq("also", "yes"),
        Seq("only"),
        Seq("no")),
      help = """Whether to include binned features in addition to or in place of
numeric features. If 'also' or 'yes', include both binned and numeric features.
If 'only', include only binned features. If 'no', include only numeric features.
Binning features involves converting numeric values to one of a limited number
of choices. Binning of most values is done logarithmically using base 2.
Binning of some fractional values is done in equal intervals. Default '%default'.

NOTE: Currently, this option does not affect any of the word-by-word rerank feature
types, which have separate '*-binned' equivalents that can be specified directly.
It does affect 'misc', 'model-compare', 'rank-score' and similar
non-word-by-word feature types. See '--rerank-features'.""")

  var rerank_top_n =
    ap.option[Int]("rerank-top-n",
      default = 50,
      must = be_>(0),
      help = """Number of top-ranked items to rerank.  Default is %default.""")

  var rerank_num_training_splits =
    ap.option[Int]("rerank-num-training-splits",
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
    if (!rerank)
      grid_lang_model_type
    else {
      val is_ngram =
        (rerank_features_all_ngram_choices.intersect(
          rerank_feature_list).size != 0)
      val is_unigram =
        (rerank_features_all_unigram_choices.intersect(
          rerank_feature_list).size != 0)
      if (is_ngram && is_unigram)
        ap.usageError("Can't have both ngram and unigram features in --rerank-features")
      else if (is_ngram)
        "ngram"
      else
        "unigram"
    }
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

  var rerank_initial_weights =
    ap.option[String]("rerank-initial-weights", "riw",
      default = "zero",
      choices = Seq("zero", "rank-score-only", "random"),
      help = """How to initialize weights during reranking. Possibilities
are 'zero' (set to all zero), 'rank-score-only' (set to zero except for
the original ranking score), 'random' (set to random).""")

  var rerank_random_restart =
    ap.option[Int]("rerank-random-restart", "rrr",
      default = 1,
      must = be_>(0),
      help = """How often to compute the reranking weights. The resulting
set of weights will be averaged. This only makes sense when
'--rerank-initialize-weights random', and implements random restarting.""")
}

trait GridLocatePerceptronParameters {
  this: GridLocateParameters =>
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

  var perceptron_rounds =
    ap.option[Int]("perceptron-rounds",
      metavar = "INT",
      default = 10000,
      must = be_>(0),
      help = """For perceptron: maximum number of training rounds
(default: %default).""")

  var perceptron_decay =
    ap.option[Double]("perceptron-decay",
      metavar = "DOUBLE",
      default = 0.0,
      must = be_within(0.0, 1.0),
      help = """Amount by which to decay the perceptron aggressiveness
factor each round. For example, the value 0.01 means to decay the factor
by 1% each round. This should be a small number, and always a number
between 0 and 1.""")
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

  var debug =
    ap.option[String]("d", "debug", metavar = "FLAGS",
      help = """Output debug info of the given types.  Multiple debug
parameters can be specified, indicating different types of info to output.
Separate parameters by spaces, commas or semicolons.  Params can be boolean,
if given alone, or valueful, if given as PARAM=VALUE.  Certain params are
list-valued; multiple values are specified by including the parameter
multiple times, or by separating values by a colon.

The best way to figure out the possible parameters is by reading the
source code. (Look for references to debug("foo") for boolean params,
debugval("foo") for valueful params, or debuglist("foo") for list-valued
params.) Some known debug flags:

gridrank: For the given test document number (starting at 1), output
a grid of the predicted rank for cells around the correct cell.
Multiple documents can have the rank output, e.g. --debug 'gridrank=45,58'
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

wordcountdocs: Regenerate document file, filtering out documents not
seen in any counts file.

some, lots, tons: General info of various sorts. (Document me.)

cell: Print out info on each cell of the Earth as it's generated.  Also
triggers some additional info during toponym resolution. (Document me.)

commontop: Extra info for debugging
 --baseline-ranker=salience-most-common-toponym.

pcl-travel: Extra info for debugging --eval-format=pcl-travel.
""")

  // Debug flags (from SphereGridEvaluator) -- need to set them
  // here before we parse the command-line debug settings. (FIXME, should
  // be a better way that introduces fewer long-range dependencies like
  // this)
  //
  //  gridrank: For the given test document number (starting at 1), output
  //            a grid of the predicted rank for cells around the true
  //            cell.  Multiple documents can have the rank output, e.g.
  //
  //            --debug 'gridrank=45,58'
  //
  //            (This will output info for documents 45 and 58.)
  //
  //  gridranksize: Size of the grid, in numbers of documents on a side.
  //                This is a single number, and the grid will be a square
  //                centered on the correct cell.
  register_list_debug_param("gridrank")
  debugval("gridranksize") = GridLocateConstants.default_gridranksize.toString

  if (debug != null)
    parse_debug_spec(debug)
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
  GridLocateRankParameters with GridLocateRerankParameters with
  GridLocatePerceptronParameters with GridLocateMiscParameters

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
    errprint("Reading stopwords from %s...", filename)
    filehand.openr(filename).toSet
  }

  protected def read_stopwords() = {
    read_stopwords_from_file(get_file_handler, params.stopwords_file,
      params.language)
  }

  lazy protected val the_stopwords = {
    if (params.no_stopwords) Set[String]()
    else read_stopwords()
  }

  protected def read_whitelist() = {
    val filehand = get_file_handler
    val wfn = params.whitelist_file
    if(wfn == null || wfn.length == 0)
      Set[String]()
    else
      filehand.openr(wfn).toSet
  }

  lazy protected val the_whitelist = read_whitelist()

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
   * Create an empty cell grid (Grid) given a document factory.
   */
  protected def create_grid(docfact: GridDocFactory[Co]): Grid[Co]

  /**
   * Read the raw training documents.  This uses the values of the parameters
   * to determine where to read the documents from and how many documents to
   * read.  A "raw document" is simply an encapsulation of the fields used
   * to create a document (as read directly from the corpus), along with the
   * schema describing the fields.
   *
   * @param docfact Document factory used to create documents.
   * @param operation Name of logical operation, to be displayed in progress
   *   messages.
   * @return Iterator over raw documents.
   */
  def read_raw_training_documents(operation: String):
      Iterator[DocStatus[Row]] = {
    val task = show_progress(operation, "training document",
        maxtime = params.max_time_per_stage,
        maxitems = params.num_training_docs)
    val dociter = params.input.toIterator.flatMapMetered(task) { dir =>
        GridDocFactory.read_raw_documents_from_textdb(get_file_handler,
          dir, "-training")
    }
    for (doc <- dociter) yield {
      val sleep_at = debugval("sleep-at-docs")
      if (sleep_at != "") {
        if (task.num_processed == sleep_at.toInt) {
          errprint("Reached %s documents, sleeping...")
          Thread.sleep(5000)
        }
      }
      doc
    }
  }

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
      get_rawdocs: String => Iterator[DocStatus[Row]]
  ) = {
    val (weights, mww) =
      if (params.weights_file != null) {
        val word_weights = mutable.Map[Gram,Double]()
        errprint("Reading word weights...")
        for (row <- TextDB.read_textdb(get_file_handler, params.weights_file)) {
          val word = row.gets("word")
          val weight = row.get[Double]("weight")
          val wordint = Unigram.memoize(word)
          if (word_weights contains wordint)
            warning("Word %s with weight %s already seen with weight %s",
              word, weight, word_weights(wordint))
          else if (weight < 0)
            warning("Word %s with invalid negative weight %s",
              word, weight)
          else if (weight >= params.ignore_weights_below)
            word_weights(wordint) = weight
        }
        // Scale the word weights to have an average of 1.
        val values = word_weights.values
        val avg = values.sum / values.size
        assert(avg > 0, "Must have at least one non-ignored non-zero weight")
        val scaled_word_weights = word_weights map {
          case (word, weight) => (word, weight / avg)
        }
        val missing_word_weight =
          if (params.missing_word_weight >= 0)
            params.missing_word_weight / avg
          else
            1.0
        errprint("Reading word weights... done.")
        (scaled_word_weights, missing_word_weight)
      } else
        (mutable.Map[Gram,Double](), 0.0)
    val lang_model_factory = create_doc_lang_model_factory(weights, mww)
    val docfact = create_document_factory(lang_model_factory)
    val grid = create_grid(docfact)
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
    grid.finish()

    if (params.salience_file != null) {
      errprint("Reading salient points...")
      for (row <- TextDB.read_textdb(get_file_handler, params.salience_file)) {
        val name = row.gets("name")
        val coord = deserialize_coord(row.gets("coord"))
        val salience = row.get[Double]("salience")
        grid.add_salient_point(coord, name, salience)
      }
      errprint("Reading salient points... done.")
    }

    if (params.output_training_cell_lang_models) {
      for (cell <- grid.iter_nonempty_cells) {
        outout(cell.format_location+"\t")
        outprint("%s", cell.grid_lm.debug_string)
      }
    }

    grid
  }

  /**
   * Create a cell grid that's populated with training data, as read from
   * the corpus (or corpora) specified in the command-line parameters.
   */
  def initialize_grid = create_grid_from_documents(read_raw_training_documents)

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
      case "naive-bayes-no-baseline" =>
        new NaiveBayesGridRanker[Co](ranker_name, grid, use_baseline = false)
      case "naive-bayes-with-baseline" =>
        new NaiveBayesGridRanker[Co](ranker_name, grid, use_baseline = true)
      case "cosine-similarity" =>
        new CosineSimilarityGridRanker[Co](ranker_name, grid, smoothed = false,
          partial = false)
      case "partial-cosine-similarity" =>
        new CosineSimilarityGridRanker[Co](ranker_name, grid, smoothed = false,
          partial = true)
      case "smoothed-cosine-similarity" =>
        new CosineSimilarityGridRanker[Co](ranker_name, grid, smoothed = true,
          partial = false)
      case "smoothed-partial-cosine-similarity" =>
        new CosineSimilarityGridRanker[Co](ranker_name, grid, smoothed = true,
          partial = true)
      case "full-kl-divergence" =>
        new KLDivergenceGridRanker[Co](ranker_name, grid, symmetric = false,
          partial = false)
      case "partial-kl-divergence" =>
        new KLDivergenceGridRanker[Co](ranker_name, grid, symmetric = false,
          partial = true)
      case "symmetric-full-kl-divergence" =>
        new KLDivergenceGridRanker[Co](ranker_name, grid, symmetric = true,
          partial = false)
      case "symmetric-partial-kl-divergence" =>
        new KLDivergenceGridRanker[Co](ranker_name, grid, symmetric = true,
          partial = true)
      case "average-cell-probability" =>
        new AverageCellProbabilityGridRanker[Co](ranker_name, grid)
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
  protected def create_pointwise_classifier_trainer = {
    val vec_factory = ArrayVector
    def create_weights(weights: VectorAggregate) = {
      errprint("Creating %s weight vector: length %s",
        params.rerank_initial_weights, weights.length)
      params.rerank_initial_weights match {
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
    params.rerank_classifier match {
      case "perceptron" | "avg-perceptron" =>
        new BasicSingleWeightMultiLabelPerceptronTrainer[GridRankerInst[Co]](
          vec_factory, params.perceptron_aggressiveness,
          decay = params.perceptron_decay,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.perceptron_rounds,
          averaged = params.rerank_classifier == "avg-perceptron") {
            override def new_weights(len: Int) =
              create_weights(new_zero_weights(len))
          }
      case "pa-perceptron" =>
        new PassiveAggressiveNoCostSingleWeightMultiLabelPerceptronTrainer[GridRankerInst[Co]](
          vec_factory, params.pa_variant, params.perceptron_aggressiveness,
          decay = params.perceptron_decay,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.perceptron_rounds) {
            override def new_weights(len: Int) =
              create_weights(new_zero_weights(len))
          }
      case "cost-perceptron" =>
        new PassiveAggressiveCostSensitiveSingleWeightMultiLabelPerceptronTrainer[GridRankerInst[Co]](
          vec_factory, params.pa_cost_type == "prediction-based",
          params.pa_variant, params.perceptron_aggressiveness,
          decay = params.perceptron_decay,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.perceptron_rounds) {
            override def new_weights(len: Int) =
              create_weights(new_zero_weights(len))
            def cost(inst: GridRankerInst[Co], correct: Int, predicted: Int) = {
              // Is this checking for correctness itself correct?  Is there a
              // problem with always returning a non-zero cost even when we
              // choose the "correct" cell?  This makes sense in that a candidate
              // is often the "best available" but not necessarily the
              // "best possible".
              if (correct == predicted) 0.0
              else inst.doc.distance_to_coord(
                     inst.candidates(predicted).get_central_point)
            }
        }
    }
  }

  /**
   * Used in conjunction with the reranker. Create a factory that
   * constructs candidate feature vectors for the reranker,
   * i.e. feature vectors measuring the compatibility between a given
   * document and a given cell.
   *
   * @see CandidateFeatVecFactory
   */
  protected def create_candidate_featvec_factory = {
    val featvec_factory = new SparseFeatureVectorFactory
    val binning_status = params.rerank_binning match {
      case "also" => BinningAlso
      case "only" => BinningOnly
      case "no" => BinningNo
    }
    def create_fact(ty: String): CandidateFeatVecFactory[Co] = {
      if (params.rerank_features_simple_unigram_choices contains ty)
        new WordCandidateFeatVecFactory[Co](featvec_factory, binning_status, ty)
      else if (params.rerank_features_matching_unigram_choices contains ty)
        new WordMatchingCandidateFeatVecFactory[Co](featvec_factory, binning_status, ty)
      else if (params.rerank_features_simple_ngram_choices contains ty)
        new NgramCandidateFeatVecFactory[Co](featvec_factory, binning_status, ty)
      else if (params.rerank_features_matching_ngram_choices contains ty)
        new NgramMatchingCandidateFeatVecFactory[Co](featvec_factory, binning_status, ty)
      else ty match {
        case "trivial" =>
          new TrivialCandidateFeatVecFactory[Co](featvec_factory, binning_status)
        case "misc" =>
          new MiscCandidateFeatVecFactory[Co](featvec_factory, binning_status)
        case "types-in-common" =>
          new TypesInCommonCandidateFeatVecFactory[Co](featvec_factory, binning_status)
        case "model-compare" =>
          new ModelCompareCandidateFeatVecFactory[Co](featvec_factory, binning_status)
        case "rank-score" =>
          new RankScoreCandidateFeatVecFactory[Co](featvec_factory, binning_status)
      }
    }

    val featlist = params.rerank_feature_list
    if (featlist.size == 0)
      params.parser.usageError("Can't specify empty --rerank-features")
    else if (featlist.size == 1)
      create_fact(featlist.head)
    else {
      val facts = featlist.map(create_fact)
      new CombiningCandidateFeatVecFactory[Co](featvec_factory, binning_status, facts)
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
      create_ranker_from_documents(read_raw_training_documents)
    if (!params.rerank) basic_ranker
    else {
      /* Factory object for generating candidate feature vectors
       * to be ranked. There is one such feature vector per cell to be
       * ranked for a given document; many of the features describe
       * the compatibility between the document and cell, e.g. of their
       * language models.
       */
      val candidate_featvec_factory = create_candidate_featvec_factory

      /* Object for training a reranker. */
      val reranker_trainer =
        new LinearClassifierGridRerankerTrainer[Co](
          create_pointwise_classifier_trainer
        ) {
          val top_n = params.rerank_top_n
          val number_of_splits = params.rerank_num_training_splits

          val feature_mapper =
            candidate_featvec_factory.featvec_factory.feature_mapper
          val label_mapper = new LabelMapper
          for (i <- 0 until params.rerank_top_n) {
            label_mapper.to_index(s"#${i + 1}")
          }

          protected def query_training_data_to_rerank_training_instances(
            data: Iterable[QueryTrainingData]
          ): Iterable[(GridRankerInst[Co], Int)] = {

            val task = new Meter("converting QTD's to", "RTI'")

            def create_candidate_featvec(query: GridDoc[Co],
                candidate: GridCell[Co], initial_score: Double,
                initial_rank: Int) = {
              val featvec =
                candidate_featvec_factory(query, candidate, initial_score,
                  initial_rank, is_training = true)
              if (debug("features")) {
                val prefix = "#%s-%s" format (task.num_processed + 1,
                  initial_rank)
                errprint("%s: Training: For query %s, candidate %s, initial score %s, initial rank %s, featvec %s",
                  prefix, query, candidate, initial_score,
                  initial_rank, featvec.pretty_print(prefix))
              }
              featvec
            }

            val maybepar_data =
              if (true /* params.no_parallel */) data
              else data.par

            maybepar_data.mapMetered(task) { qtd =>
              val agg_fv = qtd.aggregate_featvec(create_candidate_featvec)
              val label = qtd.label
              val candidates = qtd.cand_scores.map(_._1).toIndexedSeq
              (GridRankerInst(qtd.query, candidates, agg_fv), label)
            }.seq
          }

          /* Create the candidate feature vector during evaluation, by
           * invoking the candidate feature vector factory (see above).
           * This may need to operate differently than during training,
           * in particular in that new features cannot be created. Hence,
           * e.g., when handling a previously unseen word we need to skip
           * it rather than creating a previously unseen feature. The
           * reason for this is that creating a new feature would increase
           * the total size of the feature vectors and weight vector(s),
           * which we can't do after training has completed. Typically
           * the difference down to 'memoize()' vs. 'memoize_if()'.
           */
          protected def create_candidate_eval_featvec(query: GridDoc[Co],
              candidate: GridCell[Co], initial_score: Double,
              initial_rank: Int) = {
            val featvec =
              candidate_featvec_factory(query, candidate, initial_score,
                initial_rank, is_training = false)
            if (debug("features"))
              errprint("Eval: For query %s, candidate %s, initial score %s, featvec %s",
                query, candidate, initial_score, featvec)
            featvec
          }

          /* Create the initial ranker from training data. */
          protected def create_initial_ranker(
            data: Iterable[DocStatus[Row]]
          ) = create_ranker_from_documents(_ => data.toIterator)

          /* Convert encapsulated raw documents into document-cell pairs.
           */
          protected def external_instances_to_query_candidate_pairs(
            insts: Iterator[DocStatus[Row]],
            initial_ranker: Ranker[GridDoc[Co], GridCell[Co]]
          ) = {
            val grid_ranker = initial_ranker.asInstanceOf[GridRanker[Co]]
            val grid = grid_ranker.grid
            grid.docfact.raw_documents_to_documents(insts) flatMap { doc =>
              // Convert document to (doc, cell) pair.  But if a cell
              // can't be found (i.e. there were no training docs in the
              // cell of this "test" doc), skip the entire instance rather
              // than end up trying to score a fake cell
              grid.find_best_cell_for_document(doc,
                create_non_recorded = false) map ((doc, _))
            }
          }
        }

      /* Training data, in the form of an iterable over raw documents (suitably
       * wrapped in a DocStatus object). */
      val training_data = new Iterable[DocStatus[Row]] {
        def iterator =
          read_raw_training_documents(
            "reading %s for generating reranker training data")
      }.view
      /* Train the reranker. */
      reranker_trainer(training_data)
    }
  }

  /**
   * Create a grid populated from the specified training documents
   * (`create_grid_from_documents`), then create a ranker object that
   * references this grid.
   */
  def create_ranker_from_documents(
    get_rawdocs: String => Iterator[DocStatus[Row]]
  ) = {
    val grid = create_grid_from_documents(get_rawdocs)
    create_named_ranker(params.ranker, grid)
  }

  /**
   * Output, to a textdb corpus, the results of locating the best cell
   * for each document in a set of test documents.
   */
  def write_results_file(results: Iterator[DocEvalResult[Co]],
      filehand: FileHandler, base: String) {
    note_result("textdb-type", "textgrounder-results")
    write_textdb_values_with_results(filehand, base, results.map(_.to_row))
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
    }
  }
}
