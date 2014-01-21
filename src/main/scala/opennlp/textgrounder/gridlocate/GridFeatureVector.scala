///////////////////////////////////////////////////////////////////////////////
//  GridFeatureVector.scala
//
//  Copyright (C) 2012-2014 Ben Wing, The University of Texas at Austin
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

import scala.collection.mutable
import scala.math.{log,exp}

import util.debug._
import util.print._
import util.math.logn

import langmodel._
import learning._

sealed abstract class BinningStatus
case object BinningOnly extends BinningStatus
case object BinningAlso extends BinningStatus
case object BinningNo extends BinningStatus

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

  // Convert cells to label indices without regenerating strings constantly.
  val cell_to_index = mutable.Map[GridCell[Co], LabelIndex]()
  // Convert in the opposite direction.
  val index_to_cell = mutable.Map[LabelIndex, GridCell[Co]]()

  /** Convert a cell to an index. Cached for speed and to avoid excessive
    * memory generation of strings. */
  def lookup_cell(cell: GridCell[Co]) = {
    cell_to_index.get(cell) match {
      case Some(index) => index
      case None => {
        val center = cell.format_coord(cell.get_true_center)
        val label = featvec_factory.mapper.label_to_index(center)
        cell_to_index += cell -> label
        index_to_cell += label -> cell
        label
      }
    }
  }

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
        "feature %s (%s) for cell %s has disallowed value %s"
          format (feat, fvtype, cell.format_location, value))
    }
    featvec_factory.make_feature_vector(feats, lookup_cell(cell), is_training)
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
 * A factory for generating features for a doc-cell candidate consisting of
 * separate features for each word.
 */
abstract class GramByGramCandidateFeatVecFactory[Co](
  val featvec_factory: SparseFeatureVectorFactory,
  val binning_status: BinningStatus
) extends CandidateFeatVecFactory[Co] {
  /** Optionally return a per-gram feature whose count in the document is
    * `count`, with specified document and cell language models. The
    * return value is a tuple of suffix describing the particular feature
    * class being returned, and feature value. The suffix will be appended
    * to the gram itself to form the feature name. */
  def get_gram_feature(gram: Gram, doccount: Double, doclm: LangModel,
    celllm: LangModel): Option[(FeatureValue, String, Double)]

  def get_features(doc: GridDoc[Co], cell: GridCell[Co], initial_score: Double,
      initial_rank: Int) = {
    val doclm = doc.rerank_lm
    val celllm = cell.lang_model.rerank_lm
    for ((gram, count) <- doclm.iter_grams;
         (fvtype, suff, value) <- get_gram_feature(gram, count, doclm, celllm))
      yield (fvtype, "%s$%s" format (doclm.gram_to_feature(gram), suff), value)
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * features for each gram in the document that don't involve a comparison
 * between the doc and cell's gram probabilities.
 *
 * @param feattype How to compute the value assigned to the words:
 *
 * - `gram-binary`: always assign 1
 * - `gram-doc-count`: use document word count
 * - `gram-cell-count`: use cell word count
 * - `gram-doc-prob`: use document word probability
 * - `gram-cell-prob`: use cell word probability
 * - any of the above except `gram-binary` with `-binned` added, which
 *   bins logarithmically
 */
class GramCandidateFeatVecFactory[Co](
  featvec_factory: SparseFeatureVectorFactory,
  binning_status: BinningStatus,
  feattype: String
) extends GramByGramCandidateFeatVecFactory[Co](featvec_factory, binning_status) {
  def get_gram_feature(gram: Gram, doccount: Double, doclm: LangModel,
      celllm: LangModel) = {
    val binned = feattype.endsWith("-binned")
    val basetype = feattype.replace("-binned", "")
    val (fvtype, gramval) = basetype match {
      case "gram-binary" => (FeatBinary, 1.0)
      case "gram-doc-count" => (FeatCount, doccount)
      case "gram-cell-count" => (FeatCount, celllm.get_gram(gram))
      case "gram-doc-prob" => (FeatProb, doclm.gram_prob(gram))
      case "gram-cell-prob" => (FeatProb, celllm.gram_prob(gram))
    }
    if (binned)
      Some(bin_logarithmically(feattype, gramval))
    else
      Some((fvtype, feattype, gramval))
  }
}

/**
 * A factory for generating features for a doc-cell candidate consisting of
 * features for each gram in the document that involve a comparison
 * between the doc and cell's gram probabilities.
 *
 * @param feattype How to compute the value assigned to the grams that are
 *   shared:
 *
 * - `matching-gram-binary`: always assign 1
 * - `matching-gram-doc-count`: use document gram count
 * - `matching-gram-cell-count`: use cell gram count
 * - `matching-gram-doc-prob`: use document gram probability
 * - `matching-gram-cell-prob`: use cell gram probability
 * - `matching-gram-count-product`: use product of document and cell gram count
 * - `matching-gram-count-quotient`: use quotient of cell and document gram count
 * - `matching-gram-prob-product`: use product of document and cell gram prob
 * - `matching-gram-prob-quotient`: use quotient of cell and document gram prob
 * - `matching-gram-kl`: use KL-divergence component for document/cell probs
 * - any of the above with `-binned` added, which bins logarithmically
 */
class GramMatchingCandidateFeatVecFactory[Co](
  featvec_factory: SparseFeatureVectorFactory,
  binning_status: BinningStatus,
  feattype: String
) extends GramByGramCandidateFeatVecFactory[Co](featvec_factory, binning_status) {
  def get_gram_feature(gram: Gram, doccount: Double, doclm: LangModel,
      celllm: LangModel) = {
    val cellcount = celllm.get_gram(gram)
    if (cellcount == 0.0)
      None
    else {
      val binned = feattype.endsWith("-binned")
      val basetype = feattype.replace("-binned", "")
      val (fvtype, gramval) = basetype match {
        case "matching-gram-binary" => (FeatBinary, 1.0)
        case "matching-gram-doc-count" => (FeatCount, doccount)
        case "matching-gram-cell-count" => (FeatCount, cellcount)
        case "matching-gram-count-product" => (FeatCount, doccount * cellcount)
        case "matching-gram-count-quotient" => (FeatRaw, cellcount / doccount)
        case _ => {
          val docprob = doclm.gram_prob(gram)
          val cellprob = celllm.gram_prob(gram)
          basetype match {
            case "matching-gram-doc-prob" => (FeatProb, docprob)
            case "matching-gram-cell-prob" => (FeatProb, cellprob)
            case "matching-gram-prob-product" => (FeatProb, docprob * cellprob)
            case "matching-gram-prob-quotient" =>
              (FeatRescale, cellprob / docprob)
            case "matching-gram-kl" =>
              (FeatRescale, docprob*(log(docprob/cellprob)))
          }
        }
      }
      if (binned)
        Some(bin_logarithmically(feattype, gramval))
      else
        Some((fvtype, feattype, gramval))
    }
  }
}
