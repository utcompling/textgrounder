package opennlp.textgrounder
package gridlocate

import math.{log,exp}

import langmodel._
import util.debug._
import util.print._
import util.math.logn
import util.metering._
import util.text.{pretty_long, pretty_double}
import util.textdb.Row
import learning._

sealed abstract class BinningStatus
case object BinningOnly extends BinningStatus
case object BinningAlso extends BinningStatus
case object BinningNo extends BinningStatus

/**
 * A grid ranker that uses reranking.
 *
 * @tparam Co Type of document's identifying coordinate (e.g. a lat/long tuple,
 *   a year, etc.), which tends to determine the grid structure.
 * @param ranker_name Name of the ranker, for output purposes
 * @param initial_ranker Ranker used to compute initial ranking; the top items
 *   are then reranked.
 */
abstract class GridReranker[Co](
  ranker_name: String,
  _initial_ranker: GridRanker[Co]
) extends GridRanker(ranker_name + "/" + _initial_ranker.ranker_name,
  _initial_ranker.grid
) with Reranker[GridDoc[Co], GridCell[Co]] {
  val initial_ranker = _initial_ranker
}

/**
 * A factory for generating candidate feature vectors describing the
 * properties of one of the possible candidates (cells) to be chosen by
 * a reranker for a given query (i.e. document). In general, the
 * features describe the compatibility between the query and the candidate,
 * e.g. between the language models of a document and a cell.
 *
 * The factory is in the form of a function that will generate a feature
 * vector when passed appropriate arguments: a document, a cell, the score
 * of the cell as produced by the original ranker, the initial ranking of
 * the cell, and a boolean indicating whether we are generating the
 * feature vector for use in training a model or in evaluating a model.
 * (This matters in that we can only create new features, e.g. in handling
 * unseen words, during training because it will end up lengthening the
 * feature and weight vectors. This can't happen in evaluation, since the
 * weight vector was already computed during training.)
 */
trait CandidateFeatVecFactory[Co] extends (
  (GridDoc[Co], GridCell[Co], Double, Int, Boolean) => FeatureVector
) {
  /** Underlying feature-vector factory for generating features. */
  val featvec_factory: SparseFeatureVectorFactory
  /** Whether to create binned, non-binned or both types of features. */
  val binning_status: BinningStatus

  /**
   * Return an Iterable of feature-value pairs for a document-cell pair,
   * usually describing similarities between the document and cell's
   * language models. Meant to be supplied by subclasses.
   *
   * @param doc Document of document-cell pair.
   * @param cell Cell of document-cell pair.
   * @param initial_score Initial ranking score for this cell.
   * @param initial_rank Rank of this cell in the initial ranking (0-based).
   */
  def get_features(doc: GridDoc[Co], cell: GridCell[Co],
      initial_score: Double, initial_rank: Int
  ): Iterable[(FeatureValue, String, Double)]

  val logarithmic_base = 2.0

  def disallowed_value(value: Double) = value.isNaN || value.isInfinity

  /** Add a feature with the given value, binned logarithmically, i.e.
   * in place of directly including the feature's value we put the value
   * in one of a series of logarithmically-spaced bins and include a
   * binary feature indicating the presence in the correct bin.
   */
  protected def bin_logarithmically(feat: String, value: Double) = {
    // We have separate bins for the values that will cause problems (NaN,
    // positive and negative infinity, 0). In addition, we create separate
    // bins for negative values, using the same binning scheme as for
    // positive values but with "!neg" appended to the feature name.
    // (Log of positive values produces both positive and negative results
    // depending on whether the value is greater or less than 1, and log
    // of negative values is disallowed.)
    if (disallowed_value(value))
      (FeatBinary, "%s!%s" format (feat, value), 1.0)
    else if (value == 0.0)
      (FeatBinary, "%s!zero" format feat, 1.0)
    else if (value > 0) {
      val log = logn(value, logarithmic_base).toInt
      (FeatBinary, "%s!%s" format (feat, log), 1.0)
    } else {
      val log = logn(-value, logarithmic_base).toInt
      (FeatBinary, "%s!%s!neg" format (feat, log), 1.0)
    }
  }

  /** Add a feature with the given value, binned logarithmically, i.e.
   * in place of directly including the feature's value we put the value
   * in one of a series of logarithmically-spaced bins and include a
   * binary feature indicating the presence in the correct bin.
   */
  protected def include_and_bin_logarithmically(fvtype: FeatureValue,
      feat: String, value: Double) = {
    // We cannot allow NaN, +Inf or -Inf as a feature value, as they will
    // wreak havoc on error calculations. However, we can still create
    // bins noting the fact that such values were encountered.
    if (binning_status eq BinningNo) {
      if (!disallowed_value(value))
        Seq((fvtype, feat, value))
      else
        Seq()
    } else {
      val bin = bin_logarithmically(feat, value)
      if ((binning_status eq BinningOnly) || disallowed_value(value))
        Seq(bin)
      else
        Seq((fvtype, feat, value), bin_logarithmically(feat, value))
    }
  }

  // Shorthand for include_and_bin_logarithmically, which may need to be
  // called repeatedly.
  protected def ib(fvtype: FeatureValue, feat: String, value: Double) =
    include_and_bin_logarithmically(fvtype, feat, value)

  val fractional_increment = 0.1

  protected def bin_fractionally(feat: String, value: Double) = {
    assert(value >= 0 && value <= 1)
    val incr = (value / fractional_increment).toInt
    (FeatBinary, "%s!%s" format (feat, incr), 1.0)
  }

  protected def include_and_bin_fractionally(fvtype: FeatureValue,
      feat: String, value: Double) = {
    if (binning_status eq BinningNo)
      Seq((fvtype, feat, value))
    else {
      val bin = bin_fractionally(feat, value)
      if (binning_status eq BinningOnly)
        Seq(bin)
      else
        Seq((fvtype, feat, value), bin)
    }
  }

  /**
   * Generate a feature vector from a query-candidate (document-cell) pair.
   *
   * @param document Document serving as a query item
   * @param cell Cell serving as a candidate to be ranked
   * @param score Score for this cell as produced by the initial ranker
   * @param is_training Whether we are training or evaluating a model
   *   (see above)
   */
  def apply(doc: GridDoc[Co], cell: GridCell[Co], score: Double,
      initial_rank: Int, is_training: Boolean) = {
    val feats = get_features(doc, cell, score, initial_rank)
    feats.foreach { case (fvtype, feat, value) =>
      assert(!disallowed_value(value),
        "feature %s (%s) has disallowed value %s"
          format (feat, fvtype, value))
    }
    featvec_factory.make_feature_vector(feats, is_training)
  }
}

/**
 * A trivial factory for generating features for a doc-cell candidate,
 * containing no features, for testing/debugging purposes.
 */
class TrivialCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus
) extends CandidateFeatVecFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = Iterable()
}

/**
 * A factory that combines the features of a set of subsidiary factories.
 */
class CombiningCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus,
  val subsidiary_facts: Iterable[CandidateFeatVecFactory[Co]]
) extends CandidateFeatVecFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = {
    // For each subsidiary factory, retrieve its features, then add the
    // index of the factory to the feature's name to disambiguate, and
    // concatenate all features.
    if (debug("show-combined-features")) {
      errprint("Document: %s", doc)
      errprint("Cell: %s", cell)
    }
    subsidiary_facts.zipWithIndex flatMap {
      case (fact, index) => {
        val feats = fact.get_features(doc, cell, initial_score, initial_rank)
        if (!debug("show-combined-features") && !debug("tag-combined-features"))
          feats
        else feats map { case (ty, item, count) =>
          val featname =
            // Obsolete comment:
            // [FIXME! May not be necessary to memoize like this. I don't
            // think we need to unmemoize the feature names (except for
            // debugging purposes), so it's enough just to OR the index
            // onto the top bits of the word, which is already a low
            // integer due to memoization (or if we change the memoization
            // strategy in a way that generates spread-out integers, we
            // can just XOR the index onto the top bits or hash the two
            // numbers together; occasional feature clashes aren't a big
            // deal).]
            if (debug("tag-combined-features"))
              "~%s~%s" format (index, item)
            else
              item
          if (debug("show-combined-features")) {
            errprint("%s = %s", featname, count)
          }
          (ty, featname, count)
        }
      }
    }
  }
}

/**
 * A factory for generating features for a doc-cell candidate, consisting of
 * miscellaneous non-word-by-word features. Generally fairly fast.
 */
class MiscCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus
) extends CandidateFeatVecFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = {
    val doclm = doc.rerank_lm
    val celllm = cell.rerank_lm
    Iterable(
      // Next three measure size of cell.
      ib(FeatCount, "$cell-numdocs", cell.num_docs),
      ib(FeatCount, "$cell-numtypes", celllm.num_types),
      ib(FeatCount, "$cell-numtokens", celllm.num_tokens),
      // Next two measure avg size of a document in the cell.
      ib(FeatCount, "$cell-numtypes-per-doc", celllm.num_types/cell.num_docs),
      ib(FeatCount, "$cell-numtokens-per-doc", celllm.num_tokens/cell.num_docs),
      // Next two measure repetitiousness of words in documents in the cell.
      ib(FeatCount, "$cell-numtokens-per-type", celllm.num_types/celllm.num_tokens),
      ib(FeatRaw, "$cell-salience", cell.salience),
      // Next two measure avg size of doc in cell vs. query doc size.
      ib(FeatRaw, "$numtypes-quotient",
        celllm.num_types/cell.num_docs/doclm.num_types),
      ib(FeatRaw, "$numtokens-quotient",
        celllm.num_tokens/cell.num_docs/doclm.num_tokens),
      ib(FeatRaw, "$salience-quotient",
        cell.salience/doc.salience.getOrElse(0.0))
      // These apparently cause near-singular issues.
      //ib(FeatCount, "$numtypes-diff", celllm.num_types - doclm.num_types),
      //ib(FeatCount, "$numtokens-diff", celllm.num_tokens - doclm.num_tokens),
      //ib(FeatCount, "$salience-diff", cell.salience - doc.salience.getOrElse(0.0)),
      //FIXME: TOO SLOW!!!
      // ib(FeatCount, "$types-in-common", types_in_common(doclm, celllm)),
      // ib(FeatRescale, "$kldiv", doclm.kl_divergence(celllm)),
      // ib(FeatRescale, "$symmetric-kldiv", doclm.symmetric_kldiv(celllm)),
      // ib(FeatRescale, "$cossim", doclm.cosine_similarity(celllm)),
      // ib(FeatLogProb, "$nb-logprob", doclm.model_logprob(celllm))
    ).flatten
  }
}

/**
 * A factory for generating features for a doc-cell candidate, specifically
 * the \$types-in-common feature.
 */
class TypesInCommonCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus
) extends CandidateFeatVecFactory[Co] {
  protected def types_in_common(doclm: LangModel, celllm: LangModel) = {
    val doctypes = doclm.iter_keys.toSet
    val celltypes = celllm.iter_keys.toSet
    (doctypes intersect celltypes).size
  }

  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = {
    val doclm = doc.rerank_lm
    val celllm = cell.rerank_lm
    ib(FeatCount, "$types-in-common", types_in_common(doclm, celllm))
  }
}

/**
 * A factory for generating features for a doc-cell candidate that consist
 * of comparisons between the language models of the two (KL-divergence,
 * symmetric KL, cosine similarity, Naive Bayes). Fairly slow.
 */
class ModelCompareCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus
) extends CandidateFeatVecFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = {
    val doclm = doc.rerank_lm
    val celllm = cell.rerank_lm
    Iterable(
      ib(FeatRescale, "$kldiv", doclm.kl_divergence(celllm)),
      ib(FeatRescale, "$symmetric-kldiv", doclm.symmetric_kldiv(celllm)),
      ib(FeatRescale, "$cossim", doclm.cosine_similarity(celllm)),
      ib(FeatLogProb, "$nb-logprob", doclm.model_logprob(celllm))
    ).flatten
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * the original rank, ranking score and maybe the binned equivalents.
 */
class RankScoreCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus
) extends CandidateFeatVecFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = {
    val frobbed_initial_score =
      if (debug("ensure-score-positive")) {
        if (initial_score < 0)
          -initial_score
        else
          initial_score
      } else initial_score
    Iterable(
      ib(FeatRescale, "$initial-score", frobbed_initial_score)
      //ib(FeatRescale, "$log-initial-score", log(frobbed_initial_score)),
      //ib(FeatRescale, "$exp-initial-score", exp(frobbed_initial_score)),
      //ib(FeatCount, "$initial-rank", initial_rank)
    ).flatten
  }
}

/**
 * A factory for generating features for a doc-cell candidate, when the
 * --rerank-lang-model specifies a unigram model.
 */
abstract class UnigramCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus
) extends CandidateFeatVecFactory[Co] {
  /**
   * Return an Iterable of features corresponding to the given doc-cell
   * (query-candidate) pair, provided that Unigram rerank distributions
   * are being used.
   *
   * @param doc Document of document-cell pair.
   * @param doclm Unigram rerank language model of document.
   * @param cell Cell of document-cell pair.
   * @param celllm Unigram rerank language model of cell.
   */
  def get_unigram_features(doc: GridDoc[Co], doclm: UnigramLangModel,
    cell: GridCell[Co], celllm: UnigramLangModel
  ): Iterable[(FeatureValue, String, Double)]

  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = {
    val doclm = Unigram.check_unigram_lang_model(doc.rerank_lm)
    val celllm =
      Unigram.check_unigram_lang_model(cell.rerank_lm)
    get_unigram_features(doc, doclm, cell, celllm)
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * separate features for each word.
 */
abstract class WordByWordCandidateFeatVecFactory[Co](
  featvec_factory: SparseFeatureVectorFactory,
  binning_status: BinningStatus
) extends UnigramCandidateFeatVecFactory[Co](featvec_factory, binning_status) {
  /** Optionally return a per-word feature whose count in the document is
    * `count`, with specified document and cell language models. The
    * return value is a tuple of suffix describing the particular feature
    * class being returned, and feature value. The suffix will be appended
    * to the word itself to form the feature name. */
  def get_word_feature(word: Gram, doccount: Double, doclm: UnigramLangModel,
    celllm: UnigramLangModel): Option[(FeatureValue, String, Double)]

  def get_unigram_features(doc: GridDoc[Co], doclm: UnigramLangModel,
    cell: GridCell[Co], celllm: UnigramLangModel) = {
    for ((word, count) <- doclm.iter_grams;
         (fvtype, suff, value) <- get_word_feature(word, count, doclm, celllm))
      yield (fvtype, "%s$%s" format (doclm.gram_to_string(word), suff), value)
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * features for each word in the document that don't involve a comparison
 * between the doc and cell's word probabilities.
 *
 * @param feattype How to compute the value assigned to the words:
 *
 * - `unigram-cell-count`: use cell word count
 * - `unigram-cell-prob`: use cell word probability
 * - any of the above with `-binned` added, which bins logarithmically
 */
class WordCandidateFeatVecFactory[Co](
  featvec_factory: SparseFeatureVectorFactory,
  binning_status: BinningStatus,
  feattype: String
) extends WordByWordCandidateFeatVecFactory[Co](featvec_factory, binning_status) {
  def get_word_feature(word: Gram, doccount: Double, doclm: UnigramLangModel,
      celllm: UnigramLangModel) = {
    val binned = feattype.endsWith("-binned")
    val basetype = feattype.replace("-binned", "")
    val (fvtype, wordval) = basetype match {
      case "unigram-cell-count" => (FeatCount, celllm.get_gram(word))
      case "unigram-cell-prob" => (FeatProb, celllm.gram_prob(word))
    }
    if (binned)
      Some(bin_logarithmically(feattype, wordval))
    else
      Some((fvtype, feattype, wordval))
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * features for each word in the document that involve a comparison
 * between the doc and cell's word probabilities.
 *
 * @param feattype How to compute the value assigned to the words that are
 *   shared:
 *
 * - `matching-unigram-binary`: always assign 1
 * - `matching-unigram-doc-count`: use document word count
 * - `matching-unigram-cell-count`: use cell word count
 * - `matching-unigram-doc-prob`: use document word probability
 * - `matching-unigram-cell-prob`: use cell word probability
 * - `matching-unigram-count-product`: use product of document and cell word count
 * - `matching-unigram-count-quotient`: use quotient of cell and document word count
 * - `matching-unigram-prob-product`: use product of document and cell word prob
 * - `matching-unigram-prob-quotient`: use quotient of cell and document word prob
 * - `matching-unigram-kl`: use KL-divergence component for document/cell probs
 * - any of the above with `-binned` added, which bins logarithmically
 */
class WordMatchingCandidateFeatVecFactory[Co](
  featvec_factory: SparseFeatureVectorFactory,
  binning_status: BinningStatus,
  feattype: String
) extends WordByWordCandidateFeatVecFactory[Co](featvec_factory, binning_status) {
  def get_word_feature(word: Gram, doccount: Double, doclm: UnigramLangModel,
      celllm: UnigramLangModel) = {
    val cellcount = celllm.get_gram(word)
    if (cellcount == 0.0)
      None
    else {
      val binned = feattype.endsWith("-binned")
      val basetype = feattype.replace("-binned", "")
      val (fvtype, wordval) = basetype match {
        case "matching-unigram-binary" =>
          (FeatBinary, 1.0)
        case "matching-unigram-doc-count" =>
          (FeatCount, doccount)
        case "matching-unigram-cell-count" =>
          (FeatCount, cellcount)
        case "matching-unigram-count-product" =>
          (FeatCount, doccount * cellcount)
        case "matching-unigram-count-quotient" =>
          (FeatRaw, cellcount / doccount)
        case _ => {
          val docprob = doclm.gram_prob(word)
          val cellprob = celllm.gram_prob(word)
          basetype match {
            case "matching-unigram-doc-prob" =>
              (FeatProb, docprob)
            case "matching-unigram-cell-prob" =>
              (FeatProb, cellprob)
            case "matching-unigram-prob-product" =>
              (FeatProb, docprob * cellprob)
            case "matching-unigram-prob-quotient" =>
              (FeatRescale, cellprob / docprob)
            case "matching-unigram-kl" =>
              (FeatRescale, docprob*(log(docprob/cellprob)))
          }
        }
      }
      if (binned)
        Some(bin_logarithmically(feattype, wordval))
      else
        Some((fvtype, feattype, wordval))
    }
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * separate features for each n-gram.
 */
abstract class NgramByNgramCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus
) extends CandidateFeatVecFactory[Co] {
  /** Optionally return a per-ngram feature whose count in the document is
    * `doccount`, with specified document and cell language models. The
    * return value is a tuple of suffix describing the particular feature
    * class being returned, and feature value. The suffix will be appended
    * to the ngram itself to form the feature name. */
  def get_ngram_feature(word: Gram, doccount: Double, doclm: NgramLangModel,
    celllm: NgramLangModel): Option[(FeatureValue, String, Double)]

  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = {
    val doclm = Ngram.check_ngram_lang_model(doc.rerank_lm)
    val celllm =
      Ngram.check_ngram_lang_model(cell.lang_model.rerank_lm)
    for ((ngram, count) <- doclm.iter_grams;
         (fvtype, suff, value) <-
           get_ngram_feature(ngram, count, doclm, celllm))
      // Generate a feature name by concatenating the words. This may
      // conceivably lead to clashes if a word actually has the
      // separator in it, but that's unlikely and doesn't really matter
      // anyway.
      yield (fvtype,
        "%s$%s" format (Ngram.to_string(ngram) mkString "|", suff), value)
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * features for each ngram in the document that don't involve a comparison
 * between the doc and cell's ngram probabilities.
 *
 * @param feattype How to compute the value assigned to the ngrams:
 *
 * - `ngram-cell-count`: use cell ngram count
 * - `ngram-cell-prob`: use cell ngram probability
 * - any of the above with `-binned` added, which bins logarithmically
 */
class NgramCandidateFeatVecFactory[Co](
  featvec_factory: SparseFeatureVectorFactory,
  binning_status: BinningStatus,
  feattype: String
) extends NgramByNgramCandidateFeatVecFactory[Co](featvec_factory, binning_status) {
  def get_ngram_feature(ngram: Gram, doccount: Double, doclm: NgramLangModel,
      celllm: NgramLangModel) = {
    val binned = feattype.endsWith("-binned")
    val basetype = feattype.replace("-binned", "")
    val (fvtype, ngramval) = basetype match {
      case "ngram-cell-count" => (FeatCount, celllm.get_gram(ngram))
      case "ngram-cell-prob" => (FeatProb, celllm.gram_prob(ngram))
    }
    if (binned)
      Some(bin_logarithmically(feattype, ngramval))
    else
      Some((fvtype, feattype, ngramval))
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * features for each ngram in the document that involve a comparison
 * between the doc and cell's ngram probabilities.
 *
 * @param feattype How to compute the value assigned to the ngrams that are
 *   shared:
 *
 * - `matching-ngram-binary`: always assign 1
 * - `matching-ngram-doc-count`: use document ngram count
 * - `matching-ngram-cell-count`: use cell ngram count
 * - `matching-ngram-doc-prob`: use document ngram probability
 * - `matching-ngram-cell-prob`: use cell ngram probability
 * - `matching-ngram-count-product`: use product of document and cell ngram count
 * - `matching-ngram-count-quotient`: use quotient of cell and document ngram count
 * - `matching-ngram-prob-product`: use product of document and cell ngram prob
 * - `matching-ngram-prob-quotient`: use quotient of cell and document ngram prob
 * - `matching-ngram-kl`: use KL-divergence component for document/cell probs
 * - any of the above with `-binned` added, which bins logarithmically
 */
class NgramMatchingCandidateFeatVecFactory[Co](
  featvec_factory: SparseFeatureVectorFactory,
  binning_status: BinningStatus,
  feattype: String
) extends NgramByNgramCandidateFeatVecFactory[Co](featvec_factory, binning_status) {
  def get_ngram_feature(ngram: Gram, doccount: Double, doclm: NgramLangModel,
      celllm: NgramLangModel) = {
    val cellcount = celllm.get_gram(ngram)
    if (cellcount == 0.0)
      None
    else {
      val binned = feattype.endsWith("-binned")
      val basetype = feattype.replace("-binned", "")
      val (fvtype, ngramval) = basetype match {
        case "matching-ngram-binary" =>
          (FeatBinary, 1.0)
        case "matching-ngram-doc-count" =>
          (FeatCount, doccount)
        case "matching-ngram-cell-count" =>
          (FeatCount, cellcount)
        case "matching-ngram-count-product" =>
          (FeatCount, doccount * cellcount)
        case "matching-ngram-count-quotient" =>
          (FeatRaw, cellcount / doccount)
        case _ => {
          val docprob = doclm.gram_prob(ngram)
          val cellprob = celllm.gram_prob(ngram)
          basetype match {
            case "matching-ngram-doc-prob" =>
              (FeatProb, docprob)
            case "matching-ngram-cell-prob" =>
              (FeatProb, cellprob)
            case "matching-ngram-prob-product" =>
              (FeatProb, docprob * cellprob)
            case "matching-ngram-prob-quotient" =>
              (FeatRescale, cellprob / docprob)
            case "matching-ngram-kl" =>
              (FeatRescale, docprob*(log(docprob/cellprob)))
          }
        }
      }
      if (binned)
        Some(bin_logarithmically(feattype, ngramval))
      else
        Some((fvtype, feattype, ngramval))
    }
  }
}

/**
 * A grid reranker, i.e. a pointwise reranker for doing reranking in a
 * GridLocate context, based on a grid ranker (for ranking cells in a grid
 * as possible matches for a given document).
 * See `PointwiseClassifyingReranker`.
 */
abstract class RandomGridReranker[Co](ranker_name: String,
  _initial_ranker: GridRanker[Co]
) extends GridReranker[Co](ranker_name, _initial_ranker)
    with RandomReranker[GridDoc[Co], GridCell[Co]] {
}

/**
 * A grid reranker, i.e. a pointwise reranker for doing reranking in a
 * GridLocate context, based on a grid ranker (for ranking cells in a grid
 * as possible matches for a given document).
 * See `PointwiseClassifyingReranker`.
 */
abstract class PointwiseGridReranker[Co](ranker_name: String,
  _initial_ranker: GridRanker[Co]
) extends GridReranker[Co](ranker_name, _initial_ranker)
    with PointwiseClassifyingReranker[GridDoc[Co], GridCell[Co]] {
}

/**
 * A grid reranker using a linear classifier.  See `PointwiseGridReranker`.
 *
 * @param trainer Factory object for training a linear classifier used for
 *   pointwise reranking.
 */
abstract class LinearClassifierGridRerankerTrainer[Co](
  ranker_name: String,
  val trainer: SingleWeightLinearClassifierTrainer[GridRankerInst[Co]]
) extends PointwiseClassifyingRerankerTrainer[
    GridDoc[Co], GridCell[Co], DocStatus[Row], GridRankerInst[Co]
    ] { self =>
  protected def create_rerank_classifier(
    training_data: TrainingData[GridRankerInst[Co]]
  ) = {
    val data = training_data.data
    errprint("Training linear classifier ...")
    errprint("Number of training items (aggregate feature vectors): %s",
      pretty_long(data.size))
    val num_indiv_training_items =
      data.view.map(_._1.feature_vector.depth.toLong).sum
    errprint("Number of individual feature vectors in training items: %s",
      pretty_long(num_indiv_training_items))
    val num_total_feats =
      num_indiv_training_items * data.head._1.feature_vector.length
    val num_total_stored_feats =
      data.view.map(_._1.feature_vector.stored_entries.toLong).sum
    errprint("Total number of features in all training items: %s",
      pretty_long(num_total_feats))
    errprint("Avg number of features per training item: %s",
      pretty_double(num_total_feats.toDouble / data.size))
    errprint("Total number of stored features in all training items: %s",
      pretty_long(num_total_stored_feats))
    errprint("Avg number of stored features per training item: %s",
      pretty_double(num_total_stored_feats.toDouble / data.size))
    errprint("Space reduction using sparse vectors: %s times",
      pretty_double(num_total_feats.toDouble /
        num_total_stored_feats))
    trainer(training_data)
  }

  /**
   * Actually create a reranker object, given a rerank classifier and
   * initial ranker.
   */
  override protected def create_reranker(
    _rerank_classifier: ScoringClassifier,
    _initial_ranker: Ranker[GridDoc[Co], GridCell[Co]]
  ) = {
    val grid_ir = _initial_ranker.asInstanceOf[GridRanker[Co]]
    new PointwiseGridReranker[Co](ranker_name, grid_ir) {
      protected val rerank_classifier = _rerank_classifier
      val top_n = self.top_n
      protected def create_candidate_eval_featvec(query: GridDoc[Co],
          candidate: GridCell[Co], initial_score: Double, initial_rank: Int
      ) = self.create_candidate_eval_featvec(query, candidate,
          initial_score, initial_rank)
    }
  }

  /**
   * Train a reranker, based on external training data.
   */
  override def apply(training_data: Iterable[DocStatus[Row]]) =
    super.apply(training_data).
      asInstanceOf[PointwiseGridReranker[Co]]

  override def format_query_item(item: GridDoc[Co]) = {
    "%s, lm=%s" format (item, item.rerank_lm.debug_string)
  }
}
