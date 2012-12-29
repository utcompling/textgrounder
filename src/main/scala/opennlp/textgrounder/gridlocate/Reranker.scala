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

trait RerankInstFactory[Co] extends (
  (GeoDoc[Co], GeoCell[Co], Double, Boolean) => FeatureVector
) {
  val featvec_factory =
    new SparseFeatureVectorFactory[Word](word => memoizer.unmemoize(word))
  val scoreword = memoizer.memoize("-SCORE-")

  def make_feature_vector(feats: Iterable[(Word, Double)], score: Double,
      is_training: Boolean) = {
    val feats_with_score = feats ++ Iterable(scoreword -> score)
    featvec_factory.make_feature_vector(feats_with_score, is_training)
  }

  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double,
    is_training: Boolean): FeatureVector
}

/**
 * A simple factory for generating rerank instances for a document, using
 * nothing but the score passed in.
 */
class TrivialRerankInstFactory[Co] extends
    RerankInstFactory[Co] {
  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double,
    is_training: Boolean) = make_feature_vector(Iterable(), score, is_training)
}

/**
 * A factory for generating rerank instances for a document, generating
 * separate features for each word.
 */
abstract class WordByWordRerankInstFactory[Co] extends
    RerankInstFactory[Co] {
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
 * A simple factory for generating rerank instances for a document, using
 * individual KL-divergence components for each word in the document.
 */
class KLDivRerankInstFactory[Co] extends
    WordByWordRerankInstFactory[Co] {
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
 * A simple factory for generating rerank instances for a document, using
 * the presence of matching words between document and cell.
 *
 * @param value How to compute the value assigned to the words that are
 *   shared.  If "binary", always assign 1.  If "count", assign the word
 *   count.  If "probability", assign the probability (essentially, word
 *   count normalized by the number of words in the document).
 */
class WordMatchingRerankInstFactory[Co](value: String) extends
    WordByWordRerankInstFactory[Co] {
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

trait PointwiseGridReranker[Co, RerankInst]
extends GridRanker[Co]
   with PointwiseClassifyingReranker[GeoDoc[Co], GeoCell[Co], RerankInst] {
  lazy val strategy = initial_ranker.asInstanceOf[GridRanker[Co]].strategy
}

/**
 * A trivial grid reranker.  A grid reranker is a reranker based on a grid
 * ranker (for ranking cells in a grid as possible matches for a given
 * document).  This simply returns the initial score as the new score,
 * meaning that the ranking will be unchanged.
 */
class TrivialGridReranker[Co](
  val initial_ranker: Ranker[GeoDoc[Co], GeoCell[Co]],
  val top_n: Int
) extends PointwiseGridReranker[Co, Double] {
  lazy protected val rerank_classifier =
    new TrivialScoringBinaryClassifier(
        0 // FIXME: This is incorrect but doesn't matter
    )
  protected def create_rerank_evaluation_instance(item: GeoDoc[Co],
    candidate: GeoCell[Co], score: Double) = score
}

/**
 * A grid reranker using a linear classifier.  A grid reranker is a reranker
 * based on a grid ranker (for ranking cells in a grid as possible matches
 * for a given document).
 *
 * @param initial_ranker The ranker used to create the initial ranking over
 *   the grid.
 * @param trainer Factory object for training a linear classifier used for
 *   pointwise reranking.
 * @param training_data Training data used to generate training instances
 *   for the reranking linear classifier.  Generally the same as what was
 *   used to train the initial ranker.
 * @param create_rerank_instance Factory object for creating appropriate
 *   feature vectors describing the combination of document, possible
 *   cell and initial score.
 * @param top_n Number of top items to rerank.
 */
abstract class LinearClassifierGridRerankerTrainer[Co](
  val trainer: BinaryLinearClassifierTrainer
) extends PointwiseClassifyingRerankerTrainer[
    GeoDoc[Co], GeoCell[Co], FeatureVector, DocStatus[RawDocument]
    ] { self =>
  protected def create_rerank_classifier(
    data: Iterable[(FeatureVector, Boolean)]
  ) = {
    val adapted_data = data.map {
      case (inst, truefalse) => (inst, if (truefalse) 1 else 0)
    }
    errprint("Training linear classifier ...")
    errprint("Number of training items: %s", data.size)
    val num_total_feats = data.map(_._1.length.toLong).sum
    val num_total_stored_feats = data.map(_._1.stored_entries.toLong).sum
    errprint("Total number of features in all training items: %s",
      num_total_feats)
    errprint("Avg number of features per training item: %.2f",
      num_total_feats.toDouble / data.size)
    errprint("Total number of stored features in all training items: %s",
      num_total_stored_feats)
    errprint("Avg number of stored features per training item: %.2f",
      num_total_stored_feats.toDouble / data.size)
    new LinearClassifierAdapter(trainer(adapted_data, 2))
  }

  /**
   * Actually create a reranker object, given a rerank classifier and
   * initial ranker.
   */
  override protected def create_reranker(
    _rerank_classifier: ScoringBinaryClassifier[FeatureVector],
    _initial_ranker: Ranker[GeoDoc[Co], GeoCell[Co]]
  ) = {
    new PointwiseGridReranker[Co, FeatureVector] {
      protected val rerank_classifier = _rerank_classifier
      protected val initial_ranker = _initial_ranker
      val top_n = self.top_n
      protected def create_rerank_evaluation_instance(query: GeoDoc[Co],
          candidate: GeoCell[Co], initial_score: Double) = {
        self.create_rerank_evaluation_instance(query, candidate, initial_score)
      }
    }
  }

  /**
   * Train a reranker, based on external training data.
   */
  override def apply(training_data: Iterable[DocStatus[RawDocument]]) =
    super.apply(training_data).
      asInstanceOf[PointwiseGridReranker[Co, FeatureVector]]

  override def display_query_item(item: GeoDoc[Co]) = {
    "%s, dist=%s" format (item, item.dist.debug_string)
  }
}
