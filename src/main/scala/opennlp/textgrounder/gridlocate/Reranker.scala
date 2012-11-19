package opennlp.textgrounder.gridlocate

import math.log

import opennlp.textgrounder.worddist.WordDist.memoizer._
import opennlp.textgrounder.worddist.UnigramWordDist
import opennlp.textgrounder.util.printutil._
import opennlp.textgrounder.perceptron._

import GridLocateDriver.Debug._

/**
 * A basic ranker.  Given a test item, return a list of ranked answers from
 * best to worst, with a score for each.  The score must not increase from
 * any answer to the next one.
 */
trait Ranker[TestItem, Answer] {
  /**
   * Evaluate a test item, returning a list of ranked answers from best to
   * worst, with a score for each.  The score must not increase from any
   * answer to the next one.  Any answers mentioned in `include` must be
   * included in the returned list.
   */
  def evaluate(item: TestItem, include: Iterable[Answer]):
    Iterable[(Answer, Double)]
}

/**
 * A ranker for ranking cells in a grid as possible matches for a given
 * document.
 *
 * @param strategy Object encapsulating the strategy used for performing
 *   ranking.
 */
class GridRanker[Co](
  strategy: GridLocateDocumentStrategy[Co]
 ) extends Ranker[GeoDoc[Co], GeoCell[Co]] {
  def evaluate(item: GeoDoc[Co], include: Iterable[GeoCell[Co]]) =
    strategy.return_ranked_cells(item.dist, include)
}

/**
 * A reranker.  This is a particular type of ranker that involves two
 * steps: One to compute an initial ranking, and a second to do a more
 * accurate reranking of some subset of the highest-ranked answers.  The
 * idea is that a more accurate value can be computed when there are fewer
 * possible answers to distinguish -- assuming that the initial ranker
 * is able to include the correct answer near the top significantly more
 * often than actually at the top.
 */
trait Reranker[TestItem, Answer] extends Ranker[TestItem, Answer] {
  /** Ranker for generating initial ranking. */
  protected def initial_ranker: Ranker[TestItem, Answer]

  /**
   * Number of top-ranked items to submit to reranking.
   */
  protected val top_n: Int

  /**
   * Rerank the given answers, based on an initial ranking.
   */
  def rerank_answers(item: TestItem,
    initial_ranking: Iterable[(Answer, Double)]): Iterable[(Answer, Double)]

  def evaluate_with_initial_ranking(item: TestItem,
      include: Iterable[Answer]) = {
    val initial_ranking = initial_ranker.evaluate(item, include)
    val (to_rerank, others) = initial_ranking.splitAt(top_n)
    val reranking = rerank_answers(item, to_rerank) ++ others
    (initial_ranking, reranking)
  }

  def evaluate(item: TestItem, include: Iterable[Answer]) = {
    val (initial_ranking, reranking) =
      evaluate_with_initial_ranking(item, include)
    reranking
  }
}

/**
 * A pointwise reranker that uses a scoring classifier to assign a score
 * to each possible answer to be reranked.  The idea is that, for each
 * possible answer, we create test instances based on a combination of the
 * test item and answer and score the instances to determine the ranking.
 */
trait PointwiseClassifyingReranker[TestItem, RerankInstance, Answer]
    extends Reranker[TestItem, Answer] {
  /** Scoring classifier for use in reranking. */
  protected def rerank_classifier: ScoringBinaryClassifier[RerankInstance]

  /**
   * Create a reranking training instance to feed to the classifier, given
   * a test item, a potential answer from the ranker, and whether the answer
   * is correct.  These training instances will be used to train the
   * classifier.
   */
  protected val create_rerank_instance:
    (TestItem, Answer, Double) => RerankInstance

  /**
   * Generate rerank training instances for a given ranker
   * training instance.
   */
  protected def get_rerank_training_instances(item: TestItem,
      true_answer: Answer) = {
    val answers = initial_ranker.evaluate(item, Iterable(true_answer)).take(top_n)
    for {(possible_answer, score) <- answers
         is_correct = possible_answer == true_answer
        }
      yield (
        create_rerank_instance(item, possible_answer, score), is_correct)
  }

  def rerank_answers(item: TestItem,
      answers: Iterable[(Answer, Double)]) = {
    val new_scores =
      for {(answer, score) <- answers
           instance = create_rerank_instance(item, answer, score)
           new_score = rerank_classifier.score_item(instance)
          }
        yield (answer, new_score)
    new_scores.toIndexedSeq sortWith (_._2 > _._2)
  }
}

/**
 * A pointwise classifying reranker that uses training data to train the
 * classifier.  The training data consists of test items and correct
 * answers.  From each data item, a series of training instances are
 * generated as follows: (1) rank the data item to produce a set of
 * possible answers; (2) if necessary, augment the possible answers to
 * include the correct one; (3) create a training instance for each
 * combination of test item and answer, with "correct" or "incorrect"
 * indicated.
 */
trait PointwiseClassifyingRerankerWithTrainingData[
    TestItem, RerankInstance, Answer] extends
  PointwiseClassifyingReranker[TestItem, RerankInstance, Answer] {
  /**
   * Training data used to create the reranker.
   */
  protected val training_data: Iterable[(TestItem, Answer)]

  /**
   * Create the classifier used for reranking, given a set of training data
   * (in the form of pairs of reranking instances and whether they represent
   * correct answers).
   */
  protected val create_rerank_classifier:
    Iterable[(RerankInstance, Boolean)] => ScoringBinaryClassifier[RerankInstance]

  lazy protected val rerank_classifier = {
    if (training_data == null)
      errprint("null training_data")
    val rerank_training_data = training_data.flatMap {
      case (item, true_answer) => {
        if (item == null)
          errprint("null item")
        if (true_answer == null)
          errprint("null true_answer")
        get_rerank_training_instances(item, true_answer)
      }
    }
    create_rerank_classifier(rerank_training_data)
  }
}

/**
 * A scoring binary classifier.  Given a test item, return a score, with
 * higher numbers indicating greater likelihood of being "positive".
 */
trait ScoringBinaryClassifier[TestItem] {
  /**
   * The value of `minimum_positive` indicates the dividing line between values
   * that should be considered positive and those that should be considered
   * negative; typically this will be 0 or 0.5. */
  def minimum_positive: Double
  def score_item(item: TestItem): Double
}

/**
 * An adapter class converting `BinaryLinearClassifier` (from the perceptron
 * package) into a `ScoringBinaryClassifier`.
 * FIXME: Just use one of the two everywhere.
 */
class LinearClassifierAdapter (
  cfier: BinaryLinearClassifier
) extends ScoringBinaryClassifier[FeatureVector] {
  /**
   * The value of `minimum_positive` indicates the dividing line between values
   * that should be considered positive and those that should be considered
   * negative; typically this will be 0 or 0.5. */
  def minimum_positive = 0.0
  def score_item(item: FeatureVector) = cfier.binary_score(item)
}

/**
 * Trainer for `LinearClassifierAdapter`. This is a simple factory class that
 * can be used as a function.  The data passed in is in the form of pairs of
 * reranking instances and whether they represent correct answers.
 */
class LinearClassifierAdapterTrainer (
  trainer: BinaryLinearClassifierTrainer
) extends (
  Iterable[(FeatureVector, Boolean)] => ScoringBinaryClassifier[FeatureVector]
) {
  def apply(data: Iterable[(FeatureVector, Boolean)]) = {
    val adapted_data = data.map {
      case (inst, truefalse) => (inst, if (truefalse) 1 else 0)
    }
    new LinearClassifierAdapter(trainer(adapted_data, 2))
  }
}

trait GeoDocRerankInstanceFactory[Co] extends (
  (GeoDoc[Co], GeoCell[Co], Double) => FeatureVector
) {
  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double):
    FeatureVector
}

/**
 * A simple factory for generating rerank instances for a document, using
 * nothing but the score passed in.
 */
class TrivialGeoDocRerankInstanceFactory[Co] extends
    GeoDocRerankInstanceFactory[Co] {
  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double) =
    new SparseFeatureVector(Map("-SCORE-" -> score))
}

/**
 * A simple factory for generating rerank instances for a document, using
 * individual KL-divergence components for each word in the document.
 */
class KLDivGeoDocRerankInstanceFactory[Co] extends
    GeoDocRerankInstanceFactory[Co] {
  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double) = {
    val indiv_kl_vals =
      doc.dist match {
        case udist: UnigramWordDist => {
         val celldist =
           UnigramStrategy.check_unigram_dist(cell.combined_dist.word_dist)
          (for (word <- udist.model.iter_keys;
               p = udist.lookup_word(word);
               q = celldist.lookup_word(word);
               if q != 0.0)
            yield (unmemoize_string(word), p*(log(p) - log(q)))
          ).toMap
        }
        case _ =>
          throw new UnsupportedOperationException("Don't know how to rerank when not using a unigram distribution")
      }
    new SparseFeatureVector(Map("-SCORE-" -> score) ++ indiv_kl_vals)
  }
}

/**
 * A trivial scoring binary classifier that simply returns the already
 * existing score from a ranker.
 */
class TrivialScoringBinaryClassifier(
  val minimum_positive: Double
) extends ScoringBinaryClassifier[Double] {
  def score_item(item: Double) = {
    errprint("Trivial scoring item %s", item)
    item
  }
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
) extends PointwiseClassifyingReranker[
  GeoDoc[Co], Double, GeoCell[Co]
] {
  lazy protected val rerank_classifier =
    new TrivialScoringBinaryClassifier(
        0 // FIXME: This is incorrect but doesn't matter
    )
  protected val create_rerank_instance =
    (item: GeoDoc[Co], answer: GeoCell[Co], score: Double) => score
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
class LinearClassifierGridReranker[Co](
  val initial_ranker: Ranker[GeoDoc[Co], GeoCell[Co]],
  val trainer: BinaryLinearClassifierTrainer,
  val training_data: Iterable[(GeoDoc[Co], GeoCell[Co])],
  val create_rerank_instance:
    (GeoDoc[Co], GeoCell[Co], Double) => FeatureVector,
  val top_n: Int
) extends PointwiseClassifyingRerankerWithTrainingData[
  GeoDoc[Co], FeatureVector, GeoCell[Co]
  ] {
  protected val create_rerank_classifier =
    new LinearClassifierAdapterTrainer(trainer)
}
