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
 * Common methods and fields between `Reranker` and reranker trainers.
 */
trait RerankerLike[Query, Answer] {
  /**
   * Number of top-ranked items to submit to reranking.
   */
  protected val top_n: Int

  /**
   * Display a query item (typically for debugging purposes).
   */
  protected def display_query_item(item: Query) = item.toString

  /**
   * Display an answer (typically for debugging purposes).
   */
  protected def display_answer(answer: Answer) = answer.toString
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
trait Reranker[Query, Answer] extends Ranker[Query, Answer] with RerankerLike[Query, Answer] {
  /** Ranker for generating initial ranking. */
  protected def initial_ranker: Ranker[Query, Answer]

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
  protected def create_rerank_instance(query: Query, answer: Answer,
    initial_score: Double, is_training: Boolean): RerankInstance

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
 * A class for training a pointwise classifying reranker.  Training data
 * is supplied to train the classifier, consisting of query items and correct
 * answers.  From each data item, a series of training instances are
 * generated as follows: (1) rank the data item to produce a set of
 * possible answers; (2) if necessary, augment the possible answers to
 * include the correct one; (3) create a training instance for each
 * combination of query item and answer, with "correct" or "incorrect"
 * indicated.
 */
trait PointwiseClassifyingRerankerTrainer[Query, RerankInstance, Answer]
extends RerankerLike[Query, Answer] { self =>
//  /**
//   * Number of splits used in the training data, to create the reranker.
//   */
//  protected val number_of_splits: Int
//

  /**
   * Create the classifier used for reranking, given a set of training data
   * (in the form of pairs of reranking instances and whether they represent
   * correct answers).
   */
  protected def create_rerank_classifier(
    data: Iterable[(RerankInstance, Boolean)]
  ): ScoringBinaryClassifier[RerankInstance]

  /**
   * Create a reranking training instance to feed to the classifier, given
   * a query item, a potential answer from the ranker, and whether the answer
   * is correct.  These training instances will be used to train the
   * classifier.
   */
  protected def create_rerank_instance(query: Query, answer: Answer,
    initial_score: Double, is_training: Boolean): RerankInstance

  /**
   * Generate rerank training instances for a given ranker
   * training instance.
   */
  protected def get_rerank_training_instances(item: Query,
      true_answer: Answer, initial_ranker: Ranker[Query, Answer]) = {
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

//  protected val create_initial_ranker(
//    data: Iterable[(Query, Answer)]
//  ): Ranker[Query, Answer]
//
//  lazy val initial_ranker = create_initial_ranker(training_data)
//
// protected def create_splits = {
//   errprint("Computing total number of training documents ...")
//   val numitems = training_data.size
//   errprint("Total number of training documents: %s.", numitem)
//   // If number of docs is not an even multiple of number of splits,
//   // round the split size up -- we want the last split a bit smaller
//   // rather than an extra split with only a couple of items.
//   val splitsize = (numitems + number_of_splits - 1) / number_of_splits
//   errprint("Number of splits for training reranker: %s.", number_of_splits)
//   errprint("Size of each split: %s documents.", splitsize)
//   (for (rerank_split_num <- (0 until number_of_splits).toIterator) yield {
//     val split_training_data = training_data.grouped(splitsize).zipWithIndex
//     val (rerank_splits, initrank_splits) = split_training_data partition {
//       case (data, num) => num == rerank_split_num
//     }
//     val rerank_data = rerank_splits.map(_._1).flatten
//     val initrank_data = initrank_splits.map(_._1).flatten
//     val split_initial_ranker = create_initial_ranker(initrank_data)

  def train_rerank_classifier(training_data: Iterable[(Query, Answer)],
      initial_ranker: Ranker[Query, Answer]) = {
    val rerank_training_data =
      if (debug("rerank-training")) {
        training_data.zipWithIndex.flatMap {
          case ((item, true_answer), index) => {
            val prefix = "#%d: " format (index + 1)
            errprint("%sTraining item: %s", prefix, display_query_item(item))
            errprint("%sTrue answer: %s", prefix, display_answer(true_answer))
            val training_insts =
              get_rerank_training_instances(item, true_answer, initial_ranker)
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
            get_rerank_training_instances(item, true_answer, initial_ranker)
        }
      }
    create_rerank_classifier(rerank_training_data.toIndexedSeq)
  }

  def apply(training_data: Iterable[(Query, Answer)],
      initial_ranker: Ranker[Query, Answer]) = {
    val rerank_classifier =
      train_rerank_classifier(training_data, initial_ranker)
    create_reranker(rerank_classifier, initial_ranker)
  }

  def create_reranker(
    _rerank_classifier: ScoringBinaryClassifier[RerankInstance],
    _initial_ranker: Ranker[Query, Answer]
  ) = {
    new PointwiseClassifyingReranker[Query, RerankInstance, Answer] {
      val rerank_classifier = _rerank_classifier
      val initial_ranker = _initial_ranker
      val top_n = self.top_n
      protected def create_rerank_instance(query: Query, answer: Answer,
          initial_score: Double, is_training: Boolean) = {
        self.create_rerank_instance(query, answer, initial_score, is_training)
      }
      override protected def display_query_item(item: Query) =
        self.display_query_item(item)
      override protected def display_answer(answer: Answer) =
        self.display_answer(answer)
    }
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

trait GridRerankerLike[Co] extends RerankerLike[GeoDoc[Co], GeoCell[Co]] {
  override def display_query_item(item: GeoDoc[Co]) = {
    "%s, dist=%s" format (item, item.dist.debug_string)
  }
}

trait PointwiseGridReranker[Co, RerankInstance]
extends PointwiseClassifyingReranker[GeoDoc[Co], RerankInstance, GeoCell[Co]]
   with GridRerankerLike[Co] {
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
  protected def create_rerank_instance(item: GeoDoc[Co], answer: GeoCell[Co],
    score: Double, is_training: Boolean) = score
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
) extends PointwiseClassifyingRerankerTrainer[GeoDoc[Co], FeatureVector, GeoCell[Co]]
    with GridRerankerLike[Co] {
  protected def create_rerank_classifier(
    data: Iterable[(FeatureVector, Boolean)]
  ) = {
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
