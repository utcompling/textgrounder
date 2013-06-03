package opennlp.textgrounder
package gridlocate

import math.log

import worddist.WordDist._
import worddist.UnigramWordDist
import util.print._
import util.metering._
import learning._

/**
 * A ranker for ranking cells in a grid as possible matches for a given
 * document.
 *
 * @param strategy Object encapsulating the strategy used for performing
 *   ranking.
 */
trait GridRanker[Co] extends Ranker[GeoDoc[Co], GeoCell[Co]] {
  val strategy: GridLocateDocStrategy[Co]
  def grid = strategy.grid
  def evaluate(item: GeoDoc[Co], include: Iterable[GeoCell[Co]]) =
    strategy.return_ranked_cells(item.dist, include)
}

/**
 * Object encapsulating a GridLocate data instance to be used by the
 * classifier that underlies the ranker. This corresponds to a document
 * in the training corpus and serves as the main part of an RTI (rerank
 * training instance, see `PointwiseClassifyingRerankerTrainer`).
 */
case class GridRankerInst[Co](
  doc: GeoDoc[Co],
  candidates: IndexedSeq[GeoCell[Co]],
  fv: AggregateFeatureVector
) extends DataInstance {
  final def feature_vector = fv
}

/**
 * A factory for generating "candidate instances", i.e. feature vectors
 * describing the properties of one of the possible candidates (cells) to
 * be chosen by a reranker for a given query (i.e. document). Thus, a
 * candidate instance for a reranker is a query-candidate (i.e. document-cell)
 * pair, or rather a feature vector describing this pair. In general, the
 * features describe the compatibility between the query and the candidate,
 * i.e. in this case the compatibility between the word distributions
 * (language models) of a document and a cell.
 *
 * The factory is in the form of a function that will generate a feature
 * vector when passed appropriate arguments: a document, a cell, the score
 * of the cell as produced by the original ranker, and a boolean indicating
 * whether we are generating the feature vector for use in training a model
 * or in evaluating a model. (This matters e.g. in the handling of unseen
 * words, which will modify the global language model during training but
 * not evaluation.)
 */
trait CandidateInstFactory[Co] extends (
  (GeoDoc[Co], GeoCell[Co], Double, Boolean) => FeatureVector
) {
  val featvec_factory =
    new SparseFeatureVectorFactory[Word](word => memoizer.unmemoize(word))
  val scoreword = memoizer.memoize("-SCORE-")

  /**
   * Create a feature vector.
   *
   * @param feats Sequence of (name, value) pairs naming features and giving
   *   their values, specifying the primary data of the feature vector.
   * @param score Score of cell as candidate for document, as produced by
   *   the initial ranker.
   * @param is_training Whether we are in training or evaluation.
   */
  protected def make_feature_vector(feats: Iterable[(Word, Double)],
      score: Double, is_training: Boolean) = {
    val feats_with_score = feats ++ Iterable(scoreword -> score)
    featvec_factory.make_feature_vector(feats_with_score, is_training)
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
  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double,
    is_training: Boolean): FeatureVector
}

/**
 * A simple factory for generating candidate instances for a document, using
 * nothing but the score passed in.
 */
class TrivialCandidateInstFactory[Co] extends
    CandidateInstFactory[Co] {
  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double,
    is_training: Boolean) = make_feature_vector(Iterable(), score, is_training)
}

/**
 * A factory for generating candidate instances for a document, generating
 * separate features for each word.
 */
abstract class WordByWordCandidateInstFactory[Co] extends
    CandidateInstFactory[Co] {
  def get_word_feature(word: Word, count: Double, docdist: UnigramWordDist,
    celldist: UnigramWordDist): Option[Double]

  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double,
      is_training: Boolean) = {
    val indiv_features =
      doc.dist match {
        case docdist: UnigramWordDist => {
         val celldist =
           UnigramStrategy.check_unigram_dist(cell.combined_dist.word_dist)
         for ((word, count) <- docdist.model.iter_items;
              featval = get_word_feature(word, count, docdist, celldist);
              if featval != None)
           yield (word, featval.get)
        }
        case _ =>
          fixme_error(
            "Don't know how to rerank when not using a unigram distribution")
      }
    make_feature_vector(indiv_features, score, is_training)
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * individual KL-divergence components for each word in the document.
 */
class KLDivCandidateInstFactory[Co] extends
    WordByWordCandidateInstFactory[Co] {
  def get_word_feature(word: Word, count: Double, docdist: UnigramWordDist,
      celldist: UnigramWordDist) = {
    val p = docdist.lookup_word(word)
    val q = celldist.lookup_word(word)
    if (q == 0.0)
      None
    else
      Some(p*(log(p) - log(q)))
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * the presence of matching words between document and cell.
 *
 * @param value How to compute the value assigned to the words that are
 *   shared.  If "binary", always assign 1.  If "count", assign the word
 *   count.  If "probability", assign the probability (essentially, word
 *   count normalized by the number of words in the document).
 */
class WordMatchingCandidateInstFactory[Co](value: String) extends
    WordByWordCandidateInstFactory[Co] {
  def get_word_feature(word: Word, count: Double, docdist: UnigramWordDist,
      celldist: UnigramWordDist) = {
    val qcount = celldist.model.get_item(word)
    if (qcount == 0.0)
      None
    else {
      val wordval = value match {
        case "binary" => 1
        case "count" => count
        case "count-product" => count * qcount
        case "prob-product" =>
          docdist.lookup_word(word) * celldist.lookup_word(word)
        case "probability" => docdist.lookup_word(word)
        case "kl" => {
          val p = docdist.lookup_word(word)
          val q = celldist.lookup_word(word)
          p*(log(p/q))
        }
      }
      Some(wordval)
    }
  }
}

/**
 * A grid reranker, i.e. a pointwise reranker for doing reranking in a
 * GridLocate context, based on a grid ranker (for ranking cells in a grid
 * as possible matches for a given document).
 * See `PointwiseClassifyingReranker`.
 */
trait PointwiseGridReranker[Co]
extends GridRanker[Co]
   with PointwiseClassifyingReranker[GeoDoc[Co], GeoCell[Co]] {
  lazy val strategy = initial_ranker.asInstanceOf[GridRanker[Co]].strategy
}

/**
 * A grid reranker using a linear classifier.  See `PointwiseGridReranker`.
 *
 * @param trainer Factory object for training a linear classifier used for
 *   pointwise reranking.
 */
abstract class LinearClassifierGridRerankerTrainer[Co](
  val trainer: SingleWeightLinearClassifierTrainer[GridRankerInst[Co]]
) extends PointwiseClassifyingRerankerTrainer[
    GeoDoc[Co], GeoCell[Co], DocStatus[RawDocument], GridRankerInst[Co]
    ] { self =>
  protected def create_rerank_classifier(
    data: Iterable[(GridRankerInst[Co], Int)]
  ) = {
    errprint("Training linear classifier ...")
    errprint("Number of training items: %s", data.size)
    val num_total_feats =
      data.map(_._1.feature_vector.length.toLong).sum
    val num_total_stored_feats =
      data.map(_._1.feature_vector.stored_entries.toLong).sum
    errprint("Total number of features in all training items: %s",
      num_total_feats)
    errprint("Avg number of features per training item: %.2f",
      num_total_feats.toDouble / data.size)
    errprint("Total number of stored features in all training items: %s",
      num_total_stored_feats)
    errprint("Avg number of stored features per training item: %.2f",
      num_total_stored_feats.toDouble / data.size)
    trainer(data)
  }

  /**
   * Actually create a reranker object, given a rerank classifier and
   * initial ranker.
   */
  override protected def create_reranker(
    _rerank_classifier: ScoringClassifier,
    _initial_ranker: Ranker[GeoDoc[Co], GeoCell[Co]]
  ) = {
    new PointwiseGridReranker[Co] {
      protected val rerank_classifier = _rerank_classifier
      protected val initial_ranker = _initial_ranker
      val top_n = self.top_n
      protected def create_candidate_evaluation_instance(query: GeoDoc[Co],
          candidate: GeoCell[Co], initial_score: Double) = {
        self.create_candidate_evaluation_instance(query, candidate,
          initial_score)
      }
    }
  }

  /**
   * Train a reranker, based on external training data.
   */
  override def apply(training_data: Iterable[DocStatus[RawDocument]]) =
    super.apply(training_data).
      asInstanceOf[PointwiseGridReranker[Co]]

  override def format_query_item(item: GeoDoc[Co]) = {
    "%s, dist=%s" format (item, item.dist.debug_string)
  }
}
