///////////////////////////////////////////////////////////////////////////////
//  GridLocate.scala
//
//  Copyright (C) 2010, 2011 Ben Wing, The University of Texas at Austin
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

import util.argparser._
import util.distances._
import util.experiment._
import util.io.FileHandler
import util.metering._
import util.os.output_resource_usage
import util.print.errprint
import util.textdb._
import util.debug._

import learning.{ArrayVector, Ranker}
import learning.perceptron._
import worddist._

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
 * General class retrieving command-line arguments or storing programmatic
 * configuration parameters for a cell-grid-based application. The
 * parameters in here are those necessary for initializing a cell grid
 * from training documents, but not those used for geolocating test
 * documents or other applications (e.g. creating KML maps of the
 * distribution of the training docs).
 *
 * This uses an ArgParser object (stored in the `parser` field) to set
 * the parameters appropriately, e.g. from the command line. The parameters
 * themselves are stored in field variables, initialized from the ArgParser
 * object. In order to make this work, the following steps are necessary:
 *
 * <ol>
 * <li>Create an empty ArgParser.
 * <li>Create a parameter object, passing in the parser. As the
 *     "shadow fields" in this object get initialized, the parser records
 *     the valid parameters and their properties, and initializes the
 *     fields to their default values.
 * <li>Tell the parser to parse a command line.
 * <li>Create a second parameter object the same way the first one was
 *     created. This time, the fields will be initialized to the values
 *     stored in the command line.
 * </ol>
 *
 * If programmatic access is desired and the parameters are to be set
 * in some other way, just  do the first two steps, and then change any
 * parameters that should not have their default values.
 */
trait GridLocateParameters extends ArgParserParameters {
  protected val ap = parser

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
  var stopwords_file =
    ap.option[String]("stopwords-file", "sf",
      metavar = "FILE",
      help = """File containing list of stopwords.  If not specified,
a default list of English stopwords (stored in the TextGrounder distribution)
is used.""")

  var whitelist_file =
    ap.option[String]("whitelist-file", "wf",
       metavar = "FILE",
       help = """File containing a whitelist of words. If specified, ONLY
words on the list will be read from any corpora; other words will be ignored.
If not specified, all words (except those on the stopword list) will be
read.""")

  var input_corpus =
    ap.multiOption[String]("i", "input-corpus",
      metavar = "DIR",
      help = """Directory containing an input corpus.  Documents in the
corpus can be Wikipedia articles, individual tweets in Twitter, the set of all
tweets for a given user, etc.  The corpus generally contains one or more
"views" on the raw data comprising the corpus, with different views
corresponding to differing ways of representing the original text of the
documents -- as raw, word-split text; as unigram word counts; as n-gram word
counts; etc.  Each such view has a schema file and one or more document files.
The latter contains all the data for describing each document, including
title, split (training, dev or test) and other metadata, as well as the text
or word counts that are used to create the textual distribution of the
document.  The document files are laid out in a very simple database format,
consisting of one document per line, where each line is composed of a fixed
number of fields, separated by TAB characters. (E.g. one field would list
the title, another the split, another all the word counts, etc.) A separate
schema file lists the name of each expected field.  Some of these names
(e.g. "title", "split", "text", "coord") have pre-defined meanings, but
arbitrary names are allowed, so that additional corpus-specific information
can be provided (e.g. retweet info for tweets that were retweeted from some
other tweet, redirect info when a Wikipedia article is a redirect to another
article, etc.).

Multiple such files can be given by specifying the option multiple
times.""")
  var eval_file =
    ap.multiOption[String]("e", "eval-file",
      metavar = "FILE",
      help = """File or directory containing files to evaluate on.
Multiple such files/directories can be given by specifying the option multiple
times.  If a directory is given, all files in the directory will be
considered (but if an error occurs upon parsing a file, it will be ignored).
Each file is read in and then disambiguation is performed.  Not used during
document geolocation when --eval-format=internal (the default).""")

  var results =
    ap.option[String]("r", "results",
      metavar = "FILE",
      help = """If specified, prefix of file to store results into.
Results are also normally output to stderr for debugging purposes unless
`--no-results` is given.  Results are stored as a textdb corpus, i.e. two
files will be written, with extensions `.data.txt` and `.schema.txt`, with
the former storing the data as tab-separated fields and the latter naming
the fields.""")

  var num_nearest_neighbors =
    ap.option[Int]("num-nearest-neighbors", "knn", default = 4,
      help = """Number of nearest neighbors (k in kNN); default is %default.""")

  var num_top_cells_to_output =
    ap.option[Int]("num-top-cells-to-output", "num-top-cells", default = 5,
      help = """Number of nearest neighbor cells to output; default is %default;
-1 means output all""")

  var output_training_cell_dists =
    ap.flag("output-training-cell-dists", "output-training-cells",
      help = """Output the training cell distributions after they've been trained.""")

  //// Options indicating which documents to train on or evaluate
  var eval_set =
    ap.option[String]("eval-set", "es", metavar = "SET",
      default = "dev",
      aliasedChoices = Seq(Seq("dev", "devel"), Seq("test")),
      help = """Set to use for evaluation during document geolocation when
when --eval-format=internal ('dev' or 'devel' for the development set,
'test' for the test set).  Default '%default'.""")
  var num_training_docs =
    ap.option[Int]("num-training-docs", "ntrain", metavar = "NUM",
      default = 0,
      help = """Maximum number of training documents to use.
0 means no limit.  Default 0, i.e. no limit.""")
  var num_test_docs =
    ap.option[Int]("num-test-docs", "ntest", metavar = "NUM",
      default = 0,
      help = """Maximum number of test (evaluation) documents to process.
0 means no limit.  Default 0, i.e. no limit.""")
  var skip_initial_test_docs =
    ap.option[Int]("skip-initial-test-docs", "skip-initial", metavar = "NUM",
      default = 0,
      help = """Skip this many test docs at beginning.  Default 0, i.e.
don't skip any documents.""")
  var every_nth_test_doc =
    ap.option[Int]("every-nth-test-doc", "every-nth", metavar = "NUM",
      default = 1,
      help = """Only process every Nth test doc.  Default 1, i.e.
process all.""")
  //  def skip_every_n_test_docs =
  //    ap.option[Int]("skip-every-n-test-docs", "skip-n", default = 0,
  //      help = """Skip this many after each one processed.  Default 0.""")

  //// Options used when creating word distributions
  var word_dist =
    ap.option[String]("word-dist", "wd",
      default = "pseudo-good-turing",
      aliasedChoices = Seq(
        Seq("pseudo-good-turing", "pgt"),
        Seq("dirichlet"),
        Seq("jelinek-mercer", "jelinek"),
        Seq("unsmoothed-ngram")),
      help = """Type of word distribution to use.  Possibilities are
'pseudo-good-turing' (a simplified version of Good-Turing over a unigram
distribution), 'dirichlet' (Dirichlet smoothing over a unigram distribution),
'jelinek' or 'jelinek-mercer' (Jelinek-Mercer smoothing over a unigram
distribution), and 'unsmoothed-ngram' (an unsmoothed n-gram distribution).
Default '%default'.

Note that all three involve some type of discounting, i.e. taking away a
certain amount of probability mass compared with the maximum-likelihood
distribution (which estimates 0 probability for words unobserved in a
particular document), so that unobserved words can be assigned positive
probability, based on their probability across all documents (i.e. their
global distribution).  The difference is in how the discounting factor is
computed, as well as the default value for whether to do interpolation
(always mix the global distribution in) or back-off (use the global
distribution only for words not seen in the document).  Jelinek-Mercer
and Dirichlet do interpolation by default, while pseudo-Good-Turing
does back-off; but this can be overridden using --interpolate.
Jelinek-Mercer uses a fixed discounting factor; Dirichlet uses a
discounting factor that gets smaller and smaller the larger the document,
while pseudo-Good-Turing uses a discounting factor that reserves total
mass for unobserved words that is equal to the total mass observed
for words seen once.""")
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
  var jelinek_factor =
    ap.option[Double]("jelinek-factor", "jf",
      default = 0.3,
      help = """Smoothing factor when doing Jelinek-Mercer smoothing.
The higher the value, the more relative weight to give to the global
distribution vis-a-vis the document-specific distribution.  This
should be a value between 0.0 (no smoothing at all) and 1.0 (total
smoothing, i.e. use only the global distribution and ignore
document-specific distributions entirely).  Default %default.""")
  var dirichlet_factor =
    ap.option[Double]("dirichlet-factor", "df",
      default = 500,
      help = """Smoothing factor when doing Dirichlet smoothing.
The higher the value, the more relative weight to give to the global
distribution vis-a-vis the document-specific distribution.  Default
%default.""")
  var preserve_case_words =
    ap.flag("preserve-case-words", "pcw",
      help = """Don't fold the case of words used to compute and
match against document distributions.  Note that in toponym resolution,
this applies only to words in documents (currently used only in Naive Bayes
matching), not to toponyms, which are always matched case-insensitively.""")
  var no_stopwords =
    ap.flag("no-stopwords",
      help = """Don't remove any stopwords from word distributions.""")
  var minimum_word_count =
    ap.option[Int]("minimum-word-count", "mwc", metavar = "NUM",
      default = 1,
      help = """Minimum count of words to consider in word
distributions.  Words whose count is less than this value are ignored.""")
  var max_ngram =
    ap.option[Int]("max-ngram", "mn", metavar = "NUM",
      default = 0,
      help = """Maximum length of n-grams to include in an n-gram word
distribution. Any larger n-grams included in the source (e.g. corpus)
will be ignored. A value of 0 means don't filter any n-grams.  See
also `--raw-text-max-ngram`, which controls the maximum length of n-grams
generated from a raw document.""")
  var raw_text_max_ngram =
    ap.option[Int]("raw-text-max-ngram", "rdmn", metavar = "NUM",
      default = 3,
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

  //// Options relating to cells
  var center_method =
    ap.option[String]("center-method", "cm", metavar = "CENTER_METHOD",
      default = "centroid",
      choices = Seq("centroid", "center"),
      help = """Chooses whether to use true center or centroid for cell
central-point calculation. Options are either 'centroid' or 'center'.
Default '%default'.""")

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
      help = """Relative weight to assign to the baseline (prior
probability) when doing weighted Naive Bayes.  Default %default.""")

  //// Options used when doing ACP geolocation
  var lru_cache_size =
    ap.option[Int]("lru-cache-size", "lru", metavar = "SIZE",
      default = 400,
      help = """Number of entries in the LRU cache.  Default %default.
Used only when --strategy=average-cell-probability.""")

  //// Miscellaneous options for controlling internal operation
  var no_parallel =
    ap.flag("no-parallel",
      help = """If true, don't do ranking computations in parallel.""")

  //// Debugging/output options
  var max_time_per_stage =
    ap.option[Double]("max-time-per-stage", "mts", metavar = "SECONDS",
      default = 0.0,
      help = """Maximum time per stage in seconds.  If 0, no limit.
Used for testing purposes.  Default 0, i.e. no limit.""")
  var print_results =
    ap.flag("print-results", "show-results",
      help = """Show individual results for each test document.""")
  var results_by_range =
    ap.flag("results-by-range",
      help = """Show results by range (of error distances and number of
documents in correct cell).  Not on by default as counters are used for this,
and setting so many counters breaks some Hadoop installations.""")
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

gridrank: For the given test document number (starting at 1), output
a grid of the predicted rank for cells around the correct cell.
Multiple documents can have the rank output, e.g. --debug 'gridrank=45,58'
(This will output info for documents 45 and 58.) This output can be
postprocessed to generate nice graphs; this is used e.g. in Wing's thesis.

gridranksize: Size of the grid, in numbers of documents on a side.
This is a single number, and the grid will be a square centered on the
correct cell. (Default currently 11.)

kldiv: Print out words contributing most to KL divergence.

wordcountdocs: Regenerate document file, filtering out documents not
seen in any counts file.

some, lots, tons: General info of various sorts. (Document me.)

cell: Print out info on each cell of the Earth as it's generated.  Also
triggers some additional info during toponym resolution. (Document me.)

commontop: Extra info for debugging
 --baseline-strategy=link-most-common-toponym.

pcl-travel: Extra info for debugging --eval-format=pcl-travel.
""")

}

/**
 * Subclass of GridLocateParameters used specifically for assigning cells
 * to test documents based on language-model similarity (e.g. for
 * geolocation).
 */
trait GridLocateDocParameters extends GridLocateParameters {
  protected def strategy_default = "partial-kl-divergence"
  protected def strategy_choices = Seq(
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
        Seq("internal-link", "link"),
        Seq("random"),
        Seq("num-documents", "numdocs", "num-docs"))

  protected def strategy_non_baseline_help =
"""'full-kl-divergence' (or 'full-kldiv') searches for the cell where the KL
divergence between the document and cell is smallest.

'partial-kl-divergence' (or 'partial-kldiv') is similar but uses an
abbreviated KL divergence measure that only considers the words seen in the
document; empirically, this appears to work just as well as the full KL
divergence.

'naive-bayes-with-baseline' and 'naive-bayes-no-baseline' use the Naive
Bayes algorithm to match a test document against a training document (e.g.
by assuming that the words of the test document are independent of each
other, if we are using a unigram word distribution).  The variants with
the "baseline" incorporate a prior probability into the calculations, while
the non-baseline variants don't.  The baseline is currently derived from the
number of documents in a cell.  See also 'naive-bayes-weighting' and
'naive-bayes-baseline-weight' for options controlling how the different
words are weighted against each other and how the baseline and word
probabilities are weighted.

'average-cell-probability' (or 'celldist') involves computing, for each word,
a probability distribution over cells using the word distribution of each cell,
and then combining the distributions over all words in a document, weighted by
the count the word in the document.

"""

  protected def strategy_baseline_help =
"""'internal-link' (or 'link') means use number of internal links pointing to the
document or cell.

'random' means choose randomly.

'num-documents' (or 'num-docs' or 'numdocs'; only in cell-type matching) means
use number of documents in cell.

"""

  var strategy =
    ap.option[String]("s", "strategy",
      default = strategy_default,
      aliasedChoices = strategy_choices,
      help = """Strategy/strategies to use for geolocation.
""" + strategy_non_baseline_help +
"""In addition, the following "baseline" probabilities exist, which use
simple algorithms meant for comparison purposes.

""" + strategy_baseline_help +
"""Default is '%default'.""")

  //// Reranking options
  var rerank =
    ap.option[String]("rerank",
      default = "none",
      choices = Seq("none", "pointwise"),
      help = """Type of reranking to do.  Possibilities are
'none', 'pointwise' (do pointwise reranking using a classifier).  Default
is '%default'.""")

  var rerank_top_n =
    ap.option[Int]("rerank-top-n",
      default = 50,
      help = """Number of top-ranked items to rerank.  Default is %default.""")

  var rerank_num_training_splits =
    ap.option[Int]("rerank-num-training-splits",
      default = 5,
      help = """Number of splits to use when training the reranker.
The source training data is split into this many segments, and each segment
is used to construct a portion of the actual training data for the reranker
by creating an initial ranker based on the remaining segments.  This is
similar to cross-validation for evaluation purposes and serves to avoid the
problems that ensue when a given piece of data is evaluated on a machine
trained on that same data. Default is %default.""")

  var rerank_classifier =
    ap.option[String]("rerank-classifier",
      default = "perceptron",
      choices = Seq("perceptron", "avg-perceptron", "pa-perceptron",
        "cost-perceptron"),
      help = """Type of classifier to use for reranking.  Possibilities are
'perceptron' (perceptron using the basic algorithm); 'avg-perceptron'
(perceptron using the basic algorithm, where the weights from the various
rounds are averaged -- this usually improves results if the weights oscillate
around a certain error rate, rather than steadily improving); 'pa-perceptron'
(passive-aggressive perceptron, which usually leads to steady but gradually
dropping-off error rate improvements with increased number of rounds);
'cost-perceptron' (cost-sensitive passive-aggressive perceptron, using the
error distance as the cost).  Default %default.

For the perceptron classifiers, see also `--pa-variant`,
`--perceptron-error-threshold`, `--perceptron-aggressiveness` and
`--perceptron-rounds`.""")

  val rerank_features_matching_word_choices =
    Seq("unigram-binary", "unigram-count", "unigram-count-product",
        "unigram-probability", "unigram-prob-product", "kl")

  val rerank_features_matching_ngram_choices =
    Seq("ngram-binary", "ngram-count", "ngram-count-product")

  var rerank_features =
    ap.option[String]("rerank-features",
      default = "combined",
      choices = Seq("all-kl", "combined", "trivial") ++
        rerank_features_matching_word_choices ++
        rerank_features_matching_ngram_choices,
      help = """Which features to use in the reranker, to characterize the
similarity between a document and candidate cell (largely based on the
respective language models). The original ranking score for the cell always
serves as one of the features.  Possibilities are:

'trivial' (no features beyond the original score, for testing purposes);

'unigram-binary' (use the value 1 when a word exists in both document
  and cell, 0 otherwise);

'unigram-count' (use the document word count when a word exists in both
document and cell);

'unigram-count-product' (use the product of the document and cell
  word count when a word exists in both document and cell);

'unigram-probability' (use the document probability when a word exists
  in both document and cell);

'unigram-prob-product' (use the product of both the document and cell
  probability when a word exists in both document and cell);

'kl' (when a word exists in both document and cell, use the individual
  KL-divergence component score between document and cell for the word,
  else 0);

'ngram-binary' (similar to 'unigram-binary' but include features for
  N-grams up to --max-rerank-ngram);

'ngram-count' (similar to 'unigram-count' but include features for
  N-grams up to --max-rerank-ngram);

'ngram-count-product' (similar to 'unigram-count-product' but include
  N-grams up to --max-rerank-ngram);

'all-kl' (for all words in the document, use the KL-divergence score between
  document and cell -- probably not useful as distinct from plain 'kl',
  because the difference is only due to smoothing);

'combined' (use all of the features of all the previous methods).

Default %default.

Note that this only is used when --rerank=pointwise and --rerank-classifier
specifies something other than 'trivial'.""")

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
      help = """For perceptron when reranking: Total error threshold below
which training stops (default: %default).""")

  var perceptron_aggressiveness =
    ap.option[Double]("perceptron-aggressiveness",
      metavar = "DOUBLE",
      default = 0.0,
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

  var perceptron_rounds =
    ap.option[Int]("perceptron-rounds",
      metavar = "INT",
      default = 10000,
      help = """For perceptron: maximum number of training rounds
(default: %default).""")
}

/**
 * Driver class for creating cell grids over some coordinate space, with a
 * language model associated with each cell and initialized from a corpus
 * of documents by concatenating all documents located within the cell.
 * Subclasses are for particular apps, e.g. GridLocateDocDriver for
 * document-level geolocation.
 *
 * Driver classes like this have `handle_parameters` to check the
 * passed-in parameter values and `run` to do the main operation.
 * A subclass of GridLocateApp is often used to wrap the driver and
 * initialize parameters from the command line.
 */
trait GridLocateDriver[Co] extends HadoopableArgParserExperimentDriver {
  override type TParam <: GridLocateParameters

  /**
   * Set the options to those as given.  NOTE: Currently, some of the
   * fields in this structure will be changed (canonicalized).  See above.
   * If options are illegal, an error will be signaled.
   *
   * @param options Object holding options to set
   */
  def handle_parameters() {
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
    debugval("gridranksize") = "11"

    if (params.debug != null)
      parse_debug_spec(params.debug)

    need_seq(params.input_corpus, "input-corpus")

    if (params.jelinek_factor < 0.0 || params.jelinek_factor > 1.0) {
      param_error("Value for --jelinek-factor must be between 0.0 and 1.0, but is %g" format params.jelinek_factor)
    }
  }

  protected def word_dist_type = {
    if (params.word_dist == "unsmoothed-ngram") "ngram"
    else "unigram"
  }

  def word_count_field = {
    if (word_dist_type == "ngram")
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

  /** Return a function that will create a WordDistConstructor object,
   * given a WordDistFactory.
   *
   * Currently there are two factory-type objects for word distributions
   * (language models): WordDistFactory (a lower-level factory to directly
   * create WordDist objects and handle details of initializing smoothing
   * models and such) and WordDistConstructor (a high-level factory that
   * knows how to create and initialize WordDists from source data,
   * handling issues like stopwords, vocabulary filtering, etc.). The two
   * factory objects need pointers to each other, and to handle this
   * needing mutable vars, one needs to create the other in its constructor
   * function. So, rather than creating a WordDistConstructor ourselves, we
   * pass in a function to create one when creating a WordDistFactory.
   */
  protected def get_word_dist_constructor_creator =
    (factory: WordDistFactory) => {
      if (word_dist_type == "ngram")
        new DefaultNgramWordDistConstructor(
          factory,
          ignore_case = !params.preserve_case_words,
          stopwords = the_stopwords,
          whitelist = the_whitelist,
          minimum_word_count = params.minimum_word_count,
          max_ngram = params.max_ngram,
          raw_text_max_ngram = params.raw_text_max_ngram)
      else
        new DefaultUnigramWordDistConstructor(
          factory,
          ignore_case = !params.preserve_case_words,
          stopwords = the_stopwords,
          whitelist = the_whitelist,
          minimum_word_count = params.minimum_word_count)
    }

  /**
   * Create a WordDistFactory object of the appropriate kind given
   * command-line parameters. This is a factory for creating word
   * distributions, i.e. language models.
   */
  protected def create_word_dist_factory = {
    val create_constructor = get_word_dist_constructor_creator
    if (params.word_dist == "unsmoothed-ngram")
      new UnsmoothedNgramWordDistFactory(create_constructor)
    else if (params.word_dist == "dirichlet")
      new DirichletUnigramWordDistFactory(create_constructor,
        params.interpolate, params.tf_idf, params.dirichlet_factor)
    else if (params.word_dist == "jelinek-mercer")
      new JelinekMercerUnigramWordDistFactory(create_constructor,
        params.interpolate, params.tf_idf, params.jelinek_factor)
    else
      new PseudoGoodTuringUnigramWordDistFactory(create_constructor,
        params.interpolate, params.tf_idf)
  }

  /**
   * Create a document factory (GeoDocFactory) for creating documents
   * (GeoDoc), given factory for creating the word distributions (language
   * models) associated with the documents.
   */
  protected def create_document_factory(word_dist_factory: WordDistFactory):
    GeoDocFactory[Co]

  /**
   * Create an empty cell grid (GeoGrid) given a document factory.
   */
  protected def create_grid(docfact: GeoDocFactory[Co]): GeoGrid[Co]

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
      Iterator[DocStatus[RawDocument]] = {
    val task = show_progress(operation, "training document",
        maxtime = params.max_time_per_stage,
        maxitems = params.num_training_docs)
    val dociter = params.input_corpus.toIterator.flatMapMetered(task) { dir =>
        GeoDocFactory.read_raw_documents_from_textdb(get_file_handler,
          dir, "-training")
    }
    for (doc <- dociter) yield {
      val sleep_at = debugval("sleep-at-docs")
      if (sleep_at != "") {
        if (task.num_processed == sleep_at.toInt) {
          errprint("Reached %d documents, sleeping ...")
          Thread.sleep(5000)
        }
      }
      doc
    }
  }

  /**
   * Create a cell grid that's populated with the specified training data.
   * The resulting grid will have a word distribution (language model)
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
      get_rawdocs: String => Iterator[DocStatus[RawDocument]]
  ) = {
    val word_dist_factory = create_word_dist_factory
    val docfact = create_document_factory(word_dist_factory)
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
    if(params.output_training_cell_dists) {
      for(cell <- grid.iter_nonempty_cells) {
        print(cell.shortstr+"\t")
        val word_dist = cell.combined_dist.word_dist
        println(word_dist.toString)
      }
    }
    grid
  }

  /**
   * Create a cell grid that's populated with training data, as read from
   * the corpus (or corpora) specified in the command-line parameters.
   */
  def initialize_grid = create_grid_from_documents(read_raw_training_documents)
}

/**
 * Subclass of GridLocateDriver used specifically for assigning cells
 * to test documents based on language-model similarity (e.g. for
 * geolocation). This is essentially an information-retrieval approach,
 * using language models to model document similarity.
 */
trait GridLocateDocDriver[Co] extends GridLocateDriver[Co] {
  override type TParam <: GridLocateDocParameters

  override def handle_parameters() {
    super.handle_parameters()
    if (params.perceptron_aggressiveness < 0)
      param_error("Perceptron aggressiveness value should be strictly greater than zero")
    if (params.perceptron_aggressiveness == 0.0) // If default ...
      // Currently same for both regular and pa-perceptron, despite
      // differing interpretations.
      params.perceptron_aggressiveness = 1.0

  }

  /**
   * Create a strategy object, which returns a ranking over potential
   * grid cells, given a test document. This is used to locate a test
   * document in the grid (e.g. for geolocation), generally by comparing
   * the test document's word distribution (language model) to the
   * word distribution of each grid cell.
   */
  def create_strategy(stratname: String, grid: GeoGrid[Co]) = {
    stratname match {
      case "random" =>
        new RandomGridLocateDocStrategy[Co](stratname, grid)
      case "internal-link" =>
        new MostPopularGridLocateDocStrategy[Co](stratname, grid, true)
      case "num-documents" =>
        new MostPopularGridLocateDocStrategy[Co](stratname, grid, false)
      case "naive-bayes-no-baseline" =>
        new NaiveBayesDocStrategy[Co](stratname, grid, false)
      case "naive-bayes-with-baseline" =>
        new NaiveBayesDocStrategy[Co](stratname, grid, true)
      case "cosine-similarity" =>
        new CosineSimilarityStrategy[Co](stratname, grid, smoothed = false,
          partial = false)
      case "partial-cosine-similarity" =>
        new CosineSimilarityStrategy[Co](stratname, grid, smoothed = false,
          partial = true)
      case "smoothed-cosine-similarity" =>
        new CosineSimilarityStrategy[Co](stratname, grid, smoothed = true,
          partial = false)
      case "smoothed-partial-cosine-similarity" =>
        new CosineSimilarityStrategy[Co](stratname, grid, smoothed = true,
          partial = true)
      case "full-kl-divergence" =>
        new KLDivergenceStrategy[Co](stratname, grid, symmetric = false,
          partial = false)
      case "partial-kl-divergence" =>
        new KLDivergenceStrategy[Co](stratname, grid, symmetric = false,
          partial = true)
      case "symmetric-full-kl-divergence" =>
        new KLDivergenceStrategy[Co](stratname, grid, symmetric = true,
          partial = false)
      case "symmetric-partial-kl-divergence" =>
        new KLDivergenceStrategy[Co](stratname, grid, symmetric = true,
          partial = true)
      case "average-cell-probability" =>
        new AverageCellProbabilityStrategy[Co](stratname, grid)
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
   * using a strategy object -- see create_strategy). The classifier
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
    params.rerank_classifier match {
      case "perceptron" | "avg-perceptron" =>
        new BasicSingleWeightMultiLabelPerceptronTrainer[GridRankerInst[Co]](
          vec_factory, params.perceptron_aggressiveness,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.perceptron_rounds,
          averaged = params.rerank_classifier == "avg-perceptron")
      case "pa-perceptron" =>
        new PassiveAggressiveNoCostSingleWeightMultiLabelPerceptronTrainer[GridRankerInst[Co]](
          vec_factory, params.pa_variant, params.perceptron_aggressiveness,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.perceptron_rounds)
      case "cost-perceptron" =>
        new PassiveAggressiveCostSensitiveSingleWeightMultiLabelPerceptronTrainer[GridRankerInst[Co]](
          vec_factory, params.pa_cost_type == "prediction-based",
          params.pa_variant, params.perceptron_aggressiveness,
          error_threshold = params.perceptron_error_threshold,
          max_iterations = params.perceptron_rounds) {
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
   * Used in conjunction with the reranker. Create a candidate-instance
   * factory that constructs candidate feature vectors for the reranker,
   * i.e. feature vectors measuring the compatibility between a given
   * document and a given cell.
   *
   * @see CandidateInstFactory
   */
  protected def create_candidate_instance_factory = {
    def create_fact(ty: String): CandidateInstFactory[Co] = {
      if (params.rerank_features_matching_word_choices contains ty)
        new WordMatchingCandidateInstFactory[Co](ty)
      else if (params.rerank_features_matching_ngram_choices contains ty)
        new NgramMatchingCandidateInstFactory[Co](ty)
      else ty match {
        case "trivial" =>
          new TrivialCandidateInstFactory[Co]
        case "all-kl" =>
          new KLDivCandidateInstFactory[Co]
        case "combined" =>
          new CombiningCandidateInstFactory[Co](
            params.rerank_features_matching_word_choices.map(
              create_fact(_)))
      }
    }

    create_fact(params.rerank_features)
  }

  /**
   * Create a ranker object for ranking test documents. This is currently
   * the top-level entry point for training a model based on training
   * documents.
   */
  def create_ranker: GridRanker[Co] = {
    /* The basic ranker object. */
    def basic_ranker =
      new { val strategy =
              create_strategy_from_documents(read_raw_training_documents)
          } with GridRanker[Co]
    if (params.rerank == "none") basic_ranker
    else {
      /* Factory object for generating feature vectors describing
       * candidate instances (document-cell pairs) to be ranked. There is
       * one such feature vector per cell to be ranked for a given
       * document, and it measures the compatibility of the document's and
       * cell's language models.
       */
      val candidate_instance_factory = create_candidate_instance_factory

      /* Object for training a reranker. */
      val reranker_trainer =
        new LinearClassifierGridRerankerTrainer[Co](
          create_pointwise_classifier_trainer
        ) {
          val top_n = params.rerank_top_n
          val number_of_splits = params.rerank_num_training_splits

          protected def query_training_data_to_rerank_training_instances(
            data: Iterable[QueryTrainingData]
          ): Iterable[(GridRankerInst[Co], Int)] = {

            def create_candidate_featvec(query: GeoDoc[Co],
                candidate: GeoCell[Co], initial_score: Double) = {
              val featvec =
                candidate_instance_factory(query, candidate, initial_score,
                  is_training = true)
              if (debug("features"))
                errprint("Training: For query %s, candidate %s, initial score %s, featvec %s",
                  query, candidate, initial_score, featvec)
              featvec
            }

            val task = new Meter("converting", "QTD's to RTI's")
            data.mapMetered(task) { qtd =>
              val agg_fv = qtd.aggregate_featvec(create_candidate_featvec)
              val label = qtd.label
              val candidates = qtd.cand_scores.map(_._1).toIndexedSeq
              (GridRankerInst(qtd.query, candidates, agg_fv), label)
            }
          }

          /* Create the feature vector for a candidate instance (document-cell
           * pair) during evaluation, by invoking the candidate-instance
           * factory (see above). */
          protected def create_candidate_evaluation_instance(query: GeoDoc[Co],
              candidate: GeoCell[Co], initial_score: Double) = {
            val featvec =
              candidate_instance_factory(query, candidate, initial_score,
                is_training = false)
            if (debug("features"))
              errprint("Eval: For query %s, candidate %s, initial score %s, featvec %s",
                query, candidate, initial_score, featvec)
            featvec
          }

          /* Create the initial ranker from training data. */
          protected def create_initial_ranker(
            data: Iterable[DocStatus[RawDocument]]
          ) = new { val strategy =
                     create_strategy_from_documents(_ => data.toIterator) }
                with GridRanker[Co]

          /* Convert encapsulated raw documents into document-cell pairs.
           */
          protected def external_instances_to_query_candidate_pairs(
            insts: Iterator[DocStatus[RawDocument]],
            initial_ranker: Ranker[GeoDoc[Co], GeoCell[Co]]
          ) = {
            val grid_ranker = initial_ranker.asInstanceOf[GridRanker[Co]]
            val grid = grid_ranker.grid
            grid.docfact.raw_documents_to_documents(insts) flatMap { doc =>
              // Convert document to (doc, cell) pair.  But if a cell
              // can't be found (i.e. there were no training docs in the
              // cell of this "test" doc), skip the entire instance rather
              // than end up trying to score a fake cell
              grid.find_best_cell_for_document(doc, false) map ((doc, _))
            }
          }
        }

      /* Training data, in the form of an iterable over raw documents (suitably
       * wrapped in a DocStatus object). */
      val training_data = new Iterable[DocStatus[RawDocument]] {
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
   * (`create_grid_from_documents`), then create a strategy object that
   * references this grid.
   */
  def create_strategy_from_documents(
    get_rawdocs: String => Iterator[DocStatus[RawDocument]]
  ) = {
    val grid = create_grid_from_documents(get_rawdocs)
    create_strategy(params.strategy, grid)
  }

  /**
   * Output, to a textdb corpus, the results of locating the best cell
   * for each document in a set of test documents.
   */
  def write_results_file(results: Iterator[DocEvalResult[Co]],
      filehand: FileHandler, base: String) {
    note_result("corpus-type", "textgrounder-results")
    // note_result("corpus-name", opts.corpus_name)
    // note_result("generating-app", progname)
    TextDB.write_textdb(filehand, base, results.map(_.to_row),
      results_to_output, field_description)
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
