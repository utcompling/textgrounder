package opennlp.textgrounder
package gridlocate

import math.log

import worddist.WordDist._
import worddist.UnigramWordDist
import util.print._
import perceptron._

import GridLocateDriver.Debug._

/**
 * A basic ranker.  Given a query item, return a list of ranked answers from
 * best to worst, with a score for each.  The score must not increase from
 * any answer to the next one.
 */
trait Ranker[Query, Answer] {
  /**
   * Evaluate a query item, returning a list of ranked answers from best to
   * worst, with a score for each.  The score must not increase from any
   * answer to the next one.  Any answers mentioned in `include` must be
   * included in the returned list.
   */
  def evaluate(item: Query, include: Iterable[Answer]):
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
  strategy: GridLocateDocStrategy[Co]
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
trait Reranker[Query, Answer] extends Ranker[Query, Answer] {
  /** Ranker for generating initial ranking. */
  protected def initial_ranker: Ranker[Query, Answer]

  /**
   * Number of top-ranked items to submit to reranking.
   */
  protected val top_n: Int

  protected def display_query_item(item: Query) = item.toString

  protected def display_answer(answer: Answer) = answer.toString

  /**
   * Rerank the given answers, based on an initial ranking.
   */
  def rerank_answers(item: Query,
    initial_ranking: Iterable[(Answer, Double)]): Iterable[(Answer, Double)]

  def evaluate_with_initial_ranking(item: Query,
      include: Iterable[Answer]) = {
    val initial_ranking = initial_ranker.evaluate(item, include)
    val (to_rerank, others) = initial_ranking.splitAt(top_n)
    val reranking = rerank_answers(item, to_rerank) ++ others
    (initial_ranking, reranking)
  }

  def evaluate(item: Query, include: Iterable[Answer]) = {
    val (initial_ranking, reranking) =
      evaluate_with_initial_ranking(item, include)
    reranking
  }
}

/**
 * A pointwise reranker that uses a scoring classifier to assign a score
 * to each possible answer to be reranked.  The idea is that, for each
 * possible answer, we create query instances based on a combination of the
 * query item and answer and score the instances to determine the ranking.
 */
trait PointwiseClassifyingReranker[Query, RerankInstance, Answer]
    extends Reranker[Query, Answer] {
  /** Scoring classifier for use in reranking. */
  protected def rerank_classifier: ScoringBinaryClassifier[RerankInstance]

  /**
   * Create a reranking training instance to feed to the classifier, given
   * a query item, a potential answer from the ranker, and whether the answer
   * is correct.  These training instances will be used to train the
   * classifier.
   */
  protected val create_rerank_instance:
    (Query, Answer, Double, Boolean) => RerankInstance

  /**
   * Generate rerank training instances for a given ranker
   * training instance.
   */
  protected def get_rerank_training_instances(item: Query,
      true_answer: Answer) = {
    val initial_answers = initial_ranker.evaluate(item, Iterable(true_answer))
    val top_answers = initial_answers.take(top_n)
    val answers =
      if (top_answers.find(_._1 == true_answer) != None)
        top_answers
      else
        top_answers ++ Iterable(initial_answers.find(_._1 == true_answer).get)
    for {(possible_answer, score) <- answers
         is_correct = possible_answer == true_answer
        }
      yield (
        create_rerank_instance(item, possible_answer, score, true), is_correct)
  }

  def rerank_answers(item: Query,
      answers: Iterable[(Answer, Double)]) = {
    val new_scores =
      for {(answer, score) <- answers
           instance = create_rerank_instance(item, answer, score, false)
           new_score = rerank_classifier.score_item(instance)
          }
        yield (answer, new_score)
    new_scores.toIndexedSeq sortWith (_._2 > _._2)
  }
}

/**
 * A pointwise classifying reranker that uses training data to train the
 * classifier.  The training data consists of query items and correct
 * answers.  From each data item, a series of training instances are
 * generated as follows: (1) rank the data item to produce a set of
 * possible answers; (2) if necessary, augment the possible answers to
 * include the correct one; (3) create a training instance for each
 * combination of query item and answer, with "correct" or "incorrect"
 * indicated.
 */
trait PointwiseClassifyingRerankerWithTrainingData[
    Query, RerankInstance, Answer] extends
  PointwiseClassifyingReranker[Query, RerankInstance, Answer] {
  /**
   * Training data used to create the reranker.
   */
  protected val training_data: Iterator[(Query, Answer)]

  /**
   * Create the classifier used for reranking, given a set of training data
   * (in the form of pairs of reranking instances and whether they represent
   * correct answers).
   */
  protected val create_rerank_classifier:
    Iterable[(RerankInstance, Boolean)] => ScoringBinaryClassifier[RerankInstance]

  lazy protected val rerank_classifier = {
    val rerank_training_data =
      if (debug("rerank-training")) {
        training_data.zipWithIndex.flatMap {
          case ((item, true_answer), index) => {
            val prefix = "#%d: " format (index + 1)
            errprint("%sTraining item: %s", prefix, display_query_item(item))
            errprint("%sTrue answer: %s", prefix, display_answer(true_answer))
            val training_insts =
              get_rerank_training_instances(item, true_answer)
            for (((featvec, correct), instind) <- training_insts.zipWithIndex) {
              val instpref = "%s#%d: " format (prefix, instind + 1)
              val correctstr =
                if (correct) "CORRECT" else "INCORRECT"
              errprint("%s%s: %s", instpref, correctstr, featvec)
            }
            training_insts
          }
        }
      } else {
        training_data.flatMap {
          case (item, true_answer) =>
            get_rerank_training_instances(item, true_answer)
        }
      }
    create_rerank_classifier(rerank_training_data.toIndexedSeq)
  }

  /**
   * Train the reranker and return a trained version.  Note that this isn't
   * strictly necessary, because the first attempt to use the classifier
   * will automatically train it.  However, calling this function allows
   * the time at which training occurs to be controlled.
   */
  def train() = {
    // Simply accessing the classifier will train it, since it's a lazy
    // variable.
    rerank_classifier
    this
  }
}

/**
 * A scoring binary classifier.  Given a query item, return a score, with
 * higher numbers indicating greater likelihood of being "positive".
 */
trait ScoringBinaryClassifier[Query] {
  /**
   * The value of `minimum_positive` indicates the dividing line between values
   * that should be considered positive and those that should be considered
   * negative; typically this will be 0 or 0.5. */
  def minimum_positive: Double
  def score_item(item: Query): Double
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
    errprint("Training linear classifier ...")
    errprint("Number of training items: %s", data.size)
    val num_total_feats = data.map(_._1.length).sum
    val num_total_stored_feats = data.map(_._1.stored_entries).sum
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
}

trait RerankInstanceFactory[Co] extends (
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
class TrivialRerankInstanceFactory[Co] extends
    RerankInstanceFactory[Co] {
  def apply(doc: GeoDoc[Co], cell: GeoCell[Co], score: Double,
    is_training: Boolean) = make_feature_vector(Iterable(), score, is_training)
}

/**
 * A factory for generating rerank instances for a document, generating
 * separate features for each word.  using
 * individual KL-divergence components for each word in the document.
 */
abstract class WordByWordRerankInstanceFactory[Co] extends
    RerankInstanceFactory[Co] {
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
          throw new UnsupportedOperationException("Don't know how to rerank when not using a unigram distribution")
      }
    make_feature_vector(indiv_features, score, is_training)
  }
}

/**
 * A simple factory for generating rerank instances for a document, using
 * individual KL-divergence components for each word in the document.
 */
class KLDivRerankInstanceFactory[Co] extends
    WordByWordRerankInstanceFactory[Co] {
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
class WordMatchingRerankInstanceFactory[Co](value: String) extends
    WordByWordRerankInstanceFactory[Co] {
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
        case "probability" => docdist.lookup_word(word)
      }
      Some(wordval)
    }
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
    (item: GeoDoc[Co], answer: GeoCell[Co], score: Double,
      is_training: Boolean) => score
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
  val training_data: Iterator[(GeoDoc[Co], GeoCell[Co])],
  val create_rerank_instance:
    (GeoDoc[Co], GeoCell[Co], Double, Boolean) => FeatureVector,
  val top_n: Int
) extends PointwiseClassifyingRerankerWithTrainingData[
  GeoDoc[Co], FeatureVector, GeoCell[Co]
  ] {
  protected val create_rerank_classifier =
    new LinearClassifierAdapterTrainer(trainer)

  override def display_query_item(item: GeoDoc[Co]) = {
    "%s, dist=%s" format (item, item.dist.debug_string)
  }
}
