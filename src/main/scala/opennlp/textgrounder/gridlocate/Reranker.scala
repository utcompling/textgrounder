package opennlp.textgrounder
package gridlocate

import math.log

import langmodel.{LangModel,Ngram,NgramLangModel,Unigram,UnigramLangModel}
import langmodel.NgramStorage._
import LangModel._
import util.debug._
import util.print._
import util.metering._
import util.textdb.Row
import learning._

/**
 * A ranker for ranking cells in a grid as possible matches for a given
 * document (aka "grid-locating a document").
 *
 * @tparam Co Type of document's identifying coordinate (e.g. a lat/long tuple,
 *   a year, etc.), which tends to determine the grid structure.
 * @param ranker_name Name of the ranker, for output purposes
 * @param grid Grid containing the cells over which this ranker operates
 */
abstract class GridRanker[Co](
  val ranker_name: String,
  val grid: Grid[Co]
) extends Ranker[GridDoc[Co], GridCell[Co]] {
  /**
   * For a given language model (describing a test document), return
   * an Iterable of tuples, each listing a particular cell on the Earth
   * and a score of some sort.  The cells given in `include` must be
   * included in the list.  Higher scores are better.  The results should
   * be in sorted order, with better cells earlier.
   */
  def return_ranked_cells(lang_model: LangModel,
      include: Iterable[GridCell[Co]]):
    Iterable[(GridCell[Co], Double)]

  def evaluate(item: GridDoc[Co], include: Iterable[GridCell[Co]]) =
    return_ranked_cells(item.lang_model.grid_lm, include)
}

/**
 * Object encapsulating a GridLocate data instance to be used by the
 * classifier that underlies the ranker. This corresponds to a document
 * in the training corpus and serves as the main part of an RTI (rerank
 * training instance, see `PointwiseClassifyingRerankerTrainer`).
 */
case class GridRankerInst[Co](
  doc: GridDoc[Co],
  candidates: IndexedSeq[GridCell[Co]],
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
 * i.e. in this case the compatibility between the language models
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
  (GridDoc[Co], GridCell[Co], Double, Boolean) => FeatureVector
) {
  val featvec_factory =
    new SparseFeatureVectorFactory[Word](word => memoizer.unmemoize(word))
  val scoreword = memoizer.memoize("-SCORE-")

  /**
   * Return a set of feature-value pairs for a document-cell pair, describing
   * similarities between the document and cell's language models.
   * Meant to be supplied by subclasses.
   *
   * @param doc Document of document-cell pair.
   * @param cell Cell of document-cell pair.
   */
  def get_features(doc: GridDoc[Co], cell: GridCell[Co]): Iterable[(Word, Double)]

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
    is_training: Boolean) = {
    val feats_with_score =
      get_features(doc, cell) ++ Iterable(scoreword -> score)
    featvec_factory.make_feature_vector(feats_with_score, is_training)
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * nothing but the score passed in.
 */
class TrivialCandidateInstFactory[Co] extends CandidateInstFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = Iterable()
}

/**
 * A factory that combines the features of a set of subsidiary factories.
 */
class CombiningCandidateInstFactory[Co](
  val subsidiary_facts: Iterable[CandidateInstFactory[Co]]
) extends CandidateInstFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = {
    // For each subsidiary factory, retrieve its features, then add the
    // index of the factory to the feature's name to disambiguate, and
    // concatenate all features.
    if (debug("combined-features")) {
      errprint("Document: %s", doc)
      errprint("Cell: %s", cell)
    }
    subsidiary_facts.zipWithIndex flatMap { case (fact, index) =>
      fact.get_features(doc, cell) map { case (item, count) =>
        val featname =
          // FIXME! Use item_to_string to be more general.
          "%s$%s" format (memoizer.unmemoize(item), index)
        if (debug("combined-features")) {
          errprint("%s = %s", featname, count)
        }
        // FIXME! May not be necessary to memoize like this. I don't think we
        // need to unmemoize the feature names (except for debugging
        // purposes), so it's enough just to OR the index onto the top bits
        // of the word, which is already a low integer due to memoization
        // (or if we change the memoization strategy in a way that generates
        // spread-out integers, we can just XOR the index onto the top bits
        // or hash the two numbers together; occasional feature clashes
        // aren't a big deal).
        (memoizer.memoize(featname), count)
      }
    }
  }
}

/**
 * A factory for generating candidate instances for a document, generating
 * separate features for each word.
 */
abstract class WordByWordCandidateInstFactory[Co] extends
    CandidateInstFactory[Co] {
  def get_word_feature(word: Word, count: Double, doclm: UnigramLangModel,
    celldist: UnigramLangModel): Option[Double]

  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val doclm = Unigram.check_unigram_lang_model(doc.rerank_lm)
    val celldist =
      Unigram.check_unigram_lang_model(cell.lang_model.rerank_lm)
    for ((word, count) <- doclm.model.iter_items;
         featval = get_word_feature(word, count, doclm, celldist);
         if featval != None)
      yield (word, featval.get)
  }
}

/**
 * A factory for generating candidate instances for a document, generating
 * separate features for each word.
 */
abstract class NgramByNgramCandidateInstFactory[Co] extends
    CandidateInstFactory[Co] {
  def get_ngram_feature(word: Ngram, count: Double, doclm: NgramLangModel,
    celldist: NgramLangModel): Option[Double]

  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val doclm = Ngram.check_ngram_lang_model(doc.rerank_lm)
    val celldist =
      Ngram.check_ngram_lang_model(cell.lang_model.rerank_lm)
    for ((ngram, count) <- doclm.model.iter_items;
         featval = get_ngram_feature(ngram, count, doclm, celldist);
         if featval != None)
      // Generate a feature name by concatenating the words. This may
      // conceivably lead to clashes if a word actually has the
      // separator in it, but that's unlikely and doesn't really matter
      // anyway.
      yield (memoizer.memoize(ngram mkString "|"), featval.get)
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * individual KL-divergence components for each word in the document.
 */
class KLDivCandidateInstFactory[Co] extends
    WordByWordCandidateInstFactory[Co] {
  def get_word_feature(word: Word, count: Double, doclm: UnigramLangModel,
      celldist: UnigramLangModel) = {
    val p = doclm.lookup_word(word)
    val q = celldist.lookup_word(word)
    if (q == 0.0)
      None
    else
      Some(p*(log(p/q)))
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * the presence of matching words between document and cell.
 *
 * @param value How to compute the value assigned to the words that are
 *   shared:
 *
 * - `unigram-binary`: always assign 1
 * - `unigram-count`: use document word count
 * - `unigram-count-product`: use product of document and cell word count
 * - `unigram-probability`: use document word probability
 * - `unigram-prob-product`: use product of document and cell word prob
 * - `kl`: use KL-divergence component for document/cell probs
 */
class WordMatchingCandidateInstFactory[Co](value: String) extends
    WordByWordCandidateInstFactory[Co] {
  def get_word_feature(word: Word, count: Double, doclm: UnigramLangModel,
      celldist: UnigramLangModel) = {
    val qcount = celldist.model.get_item(word)
    if (qcount == 0.0)
      None
    else {
      val wordval = value match {
        case "unigram-binary" => 1
        case "unigram-count" => count
        case "unigram-count-product" => count * qcount
        case "unigram-prob-product" =>
          doclm.lookup_word(word) * celldist.lookup_word(word)
        case "unigram-probability" => doclm.lookup_word(word)
        case "kl" => {
          val p = doclm.lookup_word(word)
          val q = celldist.lookup_word(word)
          p*(log(p/q))
        }
      }
      Some(wordval)
    }
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * the presence of matching n-grams between document and cell.
 *
 * @param value How to compute the value assigned to the words that are
 *   shared:
 *
 * - `unigram-binary`: always assign 1
 * - `unigram-count`: use document word count
 * - `unigram-count-product`: use product of document and cell word count
 * - `unigram-probability`: use document word probability
 * - `unigram-prob-product`: use product of document and cell word prob
 * - `kl`: use KL-divergence component for document/cell probs
 */
class NgramMatchingCandidateInstFactory[Co](value: String) extends
    NgramByNgramCandidateInstFactory[Co] {
  def get_ngram_feature(word: Ngram, count: Double, doclm: NgramLangModel,
    celldist: NgramLangModel) = {
    val qcount = celldist.model.get_item(word)
    if (qcount == 0.0)
      None
    else {
      val wordval = value match {
        case "ngram-binary" => 1
        case "ngram-count" => count
        case "ngram-count-product" => count * qcount
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
abstract class PointwiseGridReranker[Co](ranker_name: String,
  grid: Grid[Co]
) extends GridRanker[Co](ranker_name, grid)
  with PointwiseClassifyingReranker[GridDoc[Co], GridCell[Co]] {
    def return_ranked_cells(lang_model: LangModel,
        include: Iterable[GridCell[Co]]) =
      initial_ranker.asInstanceOf[GridRanker[Co]].return_ranked_cells(
        lang_model, include)
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
    GridDoc[Co], GridCell[Co], DocStatus[Row], GridRankerInst[Co]
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
    _initial_ranker: Ranker[GridDoc[Co], GridCell[Co]]
  ) = {
    val grid_ir = _initial_ranker.asInstanceOf[GridRanker[Co]]
    new PointwiseGridReranker[Co](grid_ir.ranker_name, grid_ir.grid) {
      protected val rerank_classifier = _rerank_classifier
      protected val initial_ranker = _initial_ranker
      val top_n = self.top_n
      protected def create_candidate_evaluation_instance(query: GridDoc[Co],
          candidate: GridCell[Co], initial_score: Double) = {
        self.create_candidate_evaluation_instance(query, candidate,
          initial_score)
      }
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
