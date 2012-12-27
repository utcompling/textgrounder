///////////////////////////////////////////////////////////////////////////////
//  Ranker.scala
//
//  Copyright (C) 2012 Ben Wing, The University of Texas at Austin
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
package learning

import worddist.WordDist._
import util.metering._
import util.print._
import learning._

import gridlocate.GridLocateDriver.Debug._

/**
 * A basic ranker.  Given a query item, return a list of ranked candidates from
 * best to worst, with a score for each.  The score must not increase from
 * any candidate to the next one.
 */
trait Ranker[Query, Candidate] {
  /**
   * Evaluate a query item, returning a list of ranked candidates from best to
   * worst, with a score for each.  The score must not increase from any
   * candidate to the next one.  Any candidates mentioned in `include` must be
   * included in the returned list.
   */
  def evaluate(item: Query, include: Iterable[Candidate]):
    Iterable[(Candidate, Double)]
}

/**
 * Common methods and fields between `Reranker` and reranker trainers.
 */
trait RerankerLike[Query, Candidate] {
  /**
   * Number of top-ranked items to submit to reranking.
   */
  val top_n: Int
}

/**
 * A reranker.  This is a particular type of ranker that involves two
 * steps: One to compute an initial ranking, and a second to do a more
 * accurate reranking of some subset of the highest-ranked candidates.  The
 * idea is that a more accurate value can be computed when there are fewer
 * possible candidates to distinguish -- assuming that the initial ranker
 * is able to include the correct candidate near the top significantly more
 * often than actually at the top.
 */
trait Reranker[Query, Candidate] extends Ranker[Query, Candidate] with RerankerLike[Query, Candidate] {
  /** Ranker for generating initial ranking. */
  protected def initial_ranker: Ranker[Query, Candidate]

  /**
   * Rerank the given candidates, based on an initial ranking.
   */
  protected def rerank_candidates(item: Query,
    initial_ranking: Iterable[(Candidate, Double)]): Iterable[(Candidate, Double)]

  def evaluate_with_initial_ranking(item: Query,
      include: Iterable[Candidate]) = {
    val initial_ranking = initial_ranker.evaluate(item, include)
    val (to_rerank, others) = initial_ranking.splitAt(top_n)
    val reranking = rerank_candidates(item, to_rerank) ++ others
    (initial_ranking, reranking)
  }

  override def evaluate(item: Query, include: Iterable[Candidate]) = {
    val (initial_ranking, reranking) =
      evaluate_with_initial_ranking(item, include)
    reranking
  }
}

/**
 * A pointwise reranker that uses a scoring classifier to assign a score
 * to each possible candidate to be reranked.  The idea is that, for each
 * possible candidate, we create test instances based on a combination of the
 * query item and candidate and score the instances to determine the ranking.
 *
 * @tparam Query type of a query
 * @tparam Candidate type of a possible candidate
 */
trait PointwiseClassifyingReranker[Query, Candidate]
    extends Reranker[Query, Candidate] {
  /** Scoring classifier for use in reranking. */
  protected def rerank_classifier: ScoringClassifier

  /**
   * Create a rerank instance to feed to the classifier during evaluation,
   * given a query item, a potential candidate from the ranker, and the score
   * from the initial ranker on this candidate.
   */
  protected def create_candidate_evaluation_instance(query: Query,
    candidate: Candidate, initial_score: Double): FeatureVector

  protected def rerank_candidates(item: Query,
      scored_candidates: Iterable[(Candidate, Double)]) = {
    val cand_featvecs =
      for ((candidate, score) <- scored_candidates)
        yield create_candidate_evaluation_instance(item, candidate, score)
    val query_featvec = new AggregateFeatureVector(cand_featvecs.toIndexedSeq)
    val new_scores = rerank_classifier.score(query_featvec).toIndexedSeq
    val candidates = scored_candidates.map(_._1).toIndexedSeq
    candidates zip new_scores sortWith (_._2 > _._2)
  }
}

/**
 * A class for training a pointwise classifying reranker.
 *
 * There are various types of training data:
 * -- External training data is the data supplied externally (e.g. from a
 *    corpus), consisting of items of generic type `ExtInst`.
 * -- Initial-ranker training data is the data used to train an initial
 *    ranker, which supplies the base ranking upon which the reranker builds.
 *    This also consists of ExtInst items and is taken directly from the
 *    external training data or a subset.
 * -- Reranker training data is the data used to train a rerank classifier,
 *    i.e. the classifier used to compute the scores that underlie the
 *    reranking. This consists of (AggregateFeatureVector, Int) pairs, i.e.
 *    pairs of aggregate feature vectors (encompassing individual feature
 *    vectors for each possible candidate, each one computed for a given
 *    query-candidate pair) and the label of the correct candidate.
 *
 * Generally, a data instance of type `ExtInst` in the external training
 * data will describe an object of type `Query`, along with the correct
 * candidate. However, it need not merely be a pair of `Query` and
 * `Candidate`. In particular, the `ExtInst` data may be the raw-data form
 * of an object that can only be created with respect to some particular
 * training corpus (e.g.  for doing back-off when computing word
 * distributions).  Hence, a method is provided to convert from one to the
 * other, given a trained initial ranker against which the "cooked" query
 * object will be looked up. For example, in the case of GridLocate,
 * `ExtInst` is of type `DocStatus[RawDocument]`, which directly describes
 * the data read from a corpus, while `Query` is of type `GeoDoc[Co]`,
 * which contains (among other things) a word distribution with statistics
 * computed using back-off, just as described above.
 *
 * Training is as follows:
 *
 * 1. External training data is supplied by the caller.
 *
 * 2. Split this data into N parts (`number_of_splits`).  Process each part
 *    in turn.  For a given part, its data will be used to generate rerank
 *    training data, using an initial ranker trained on the remaining parts.
 *
 * 3. From each data item in the external training data, a series of rerank
 *    training instances are generated as follows:
 *
 *    (1) rank the data item using the initial ranker to produce a set of
 *        possible candidates;
 *    (2) if necessary, augment the possible candidates to include the correct
 *        one;
 *    (3) create a rerank training instance for each combination of query item
 *        and candidate, with "correct" or "incorrect" indicated.
 *
 * @tparam Query type of a query
 * @tparam Candidate type of a possible candidate
 * @tparam ExtInst type of the instances used in external training data
 */
trait PointwiseClassifyingRerankerTrainer[Query, Candidate, ExtInst]
extends RerankerLike[Query, Candidate] { self =>

  case class QueryTrainingData(query: Query, correct: Candidate,
      candidates: Iterable[(Candidate, FeatureVector)]) {
    /**
     * Return the label (zero-based index) of correct candidate among the
     * candidate list.  Assertion failure if not found, since correct candidate
     * should always be in candidate list.
     */
    def label: Int = {
      val maybe_label = candidates.zipWithIndex.find {
        case ((cand, featvec), index) => cand == correct
      }
      assert(maybe_label != None, "Correct candidate should be in candidate list")
      val (_, the_label) = maybe_label.get
      the_label
    }
  }

  /**
   * Number of splits used in the training data, to create the reranker.
   */
  val number_of_splits: Int

  /**
   * Create the classifier used for reranking, given a set of rerank training
   * data.
   */
  protected def create_rerank_classifier(
    data: Iterable[(FeatureVector, Int)]
  ): ScoringClassifier

  /**
   * Create a rerank training instance to train the classifier, given a query
   * item, a potential candidate from the ranker, and the score from the initial
   * ranker on this candidate.
   */
  protected def create_candidate_training_instance(query: Query,
    candidate: Candidate, initial_score: Double): FeatureVector

  /**
   * Create a rerank instance to feed to the classifier during evaluation,
   * given a query item, a potential candidate from the ranker, and the score
   * from the initial ranker on this candidate.  Note that this function may need
   * to work differently from the corresponding function used during training
   * (e.g. in the handling of previously unseen words).
   */
  protected def create_candidate_evaluation_instance(query: Query,
    candidate: Candidate, initial_score: Double): FeatureVector

  /**
   * Create an initial ranker based on initial-ranker training data.
   */
  protected def create_initial_ranker(data: Iterable[ExtInst]):
    Ranker[Query, Candidate]

  /**
   * Convert an external instance into a query-candidate pair, if possible,
   * given the external instance and the initial ranker trained from other
   * external instances.
   */
  protected def external_instances_to_query_candidate_pairs(
    inst: Iterator[ExtInst], initial_ranker: Ranker[Query, Candidate]
  ): Iterator[(Query, Candidate)]

  protected def query_training_data_to_rerank_training_instance(
    qtd: QueryTrainingData
  ): (AggregateFeatureVector, Int) = {
    (new AggregateFeatureVector(qtd.candidates.map(_._2).toIndexedSeq), qtd.label)
  }

  protected def default_query_training_data_to_rerank_training_instances(
    data: Iterable[QueryTrainingData]
  ): Iterable[(AggregateFeatureVector, Int)] = {
    for (qtd <- data) yield
      query_training_data_to_rerank_training_instance(qtd)
  }

  /**
   * Convert an external instance into a query-candidate pair, if possible,
   * given the external instance and the initial ranker trained from other
   * external instances.
   */
  protected def query_training_data_to_rerank_training_instances(
    data: Iterable[QueryTrainingData]
  ): Iterable[(AggregateFeatureVector, Int)] =
    default_query_training_data_to_rerank_training_instances(data)

  /**
   * Display a query item (typically for debugging purposes).
   */
  def display_query_item(item: Query) = item.toString

  /**
   * Display an candidate (typically for debugging purposes).
   */
  def display_candidate(candidate: Candidate) = candidate.toString

  /**
   * Generate rerank training instances for a given instance from the
   * external training data.
   *
   * @param initial_ranker Initial ranker used to score the candidate.
   */
  protected def get_query_training_data(
    query: Query, correct: Candidate, initial_ranker: Ranker[Query, Candidate]
  ) = {
    val initial_candidates =
      initial_ranker.evaluate(query, Iterable(correct))
    val top_candidates = initial_candidates.take(top_n)
    val cand_scores =
      if (top_candidates.find(_._1 == correct) != None)
        top_candidates
      else
        top_candidates ++
          Iterable(initial_candidates.find(_._1 == correct).get)
    val cand_featvecs =
      for ((cand, score) <- cand_scores) yield
        (cand, create_candidate_training_instance(query, cand, score))
    QueryTrainingData(query, correct, cand_featvecs)
  }

  /**
   * Given one of the splits of external training data and an initial ranker
   * created from the remainder, generate the corresponding rerank training
   * data.
   */
  protected def get_rerank_training_data_for_split(
      splitnum: Int, data: Iterator[ExtInst],
      initial_ranker: Ranker[Query, Candidate]
    ): Iterator[QueryTrainingData] = {
    val query_candidate_pairs =
      external_instances_to_query_candidate_pairs(data, initial_ranker)
    val task = new Meter("generating",
      "split-#%d rerank training instance" format splitnum)
    if (debug("rerank-training")) {
      query_candidate_pairs.zipWithIndex.mapMetered(task) {
        case ((query, correct), index) => {
          val prefix = "#%d: " format (index + 1)
          errprint("%sTraining item: %s", prefix, display_query_item(query))
          errprint("%sTrue candidate: %s", prefix, display_candidate(correct))
          val query_training_data =
            get_query_training_data(query, correct, initial_ranker)
          for (((candidate, featvec), candind) <-
              query_training_data.candidates.zipWithIndex) {
            val instpref = "%s#%d: " format (prefix, candind + 1)
            val correctstr =
              if (correct == candidate) "CORRECT" else "INCORRECT"
            errprint("%s%s: (%s, %s)", instpref, correctstr,
              candidate, featvec)
          }
          query_training_data
        }
      }
    } else {
      query_candidate_pairs.mapMetered(task) {
        case (query, correct) =>
          get_query_training_data(query, correct, initial_ranker)
      }
    }
  }

  /**
   * Given external training data, generate the corresponding rerank training
   * data.  This involves splitting up the rerank data into parts and training
   * separate initial rankers, as described above.
   */
  protected def get_rerank_training_data(training_data: Iterable[ExtInst]
  ): Iterable[QueryTrainingData] = {
    val numitems = training_data.size
    errprint("Total number of training documents: %s.", numitems)
    // If number of docs is not an even multiple of number of splits,
    // round the split size up -- we want the last split a bit smaller
    // rather than an extra split with only a couple of items.
    val splitsize = (numitems + number_of_splits - 1) / number_of_splits
    errprint("Number of splits for training reranker: %s.", number_of_splits)
    errprint("Size of each split: %s documents.", splitsize)
    (0 until number_of_splits) flatMap { rerank_split_num =>
      errprint("Generating data for split %s" format rerank_split_num)
      val split_training_data = training_data.grouped(splitsize).zipWithIndex
      val (rerank_splits, initrank_splits) = split_training_data partition {
        case (data, num) => num == rerank_split_num
      }
      val rerank_data = rerank_splits.flatMap(_._1)
      val initrank_data = initrank_splits.flatMap(_._1).toIterable
      val split_initial_ranker = create_initial_ranker(initrank_data)
      get_rerank_training_data_for_split(rerank_split_num, rerank_data,
        split_initial_ranker).toIterable
    }
  }

  /**
   * Train a rerank classifier, based on external training data.
   */
  protected def train_rerank_classifier(
    training_data: Iterable[ExtInst]
  ) = {
    val rerank_training_data = get_rerank_training_data(training_data)
    val rerank_instances =
      query_training_data_to_rerank_training_instances(rerank_training_data)
    create_rerank_classifier(rerank_instances)
  }

  /**
   * Actually create a reranker object, given a rerank classifier and
   * initial ranker.
   */
  protected def create_reranker(
    _rerank_classifier: ScoringClassifier,
    _initial_ranker: Ranker[Query, Candidate]
  ) = {
    new PointwiseClassifyingReranker[Query, Candidate] {
      protected val rerank_classifier = _rerank_classifier
      protected val initial_ranker = _initial_ranker
      val top_n = self.top_n
      protected def create_candidate_evaluation_instance(query: Query,
          candidate: Candidate, initial_score: Double) = {
        self.create_candidate_evaluation_instance(query, candidate, initial_score)
      }
    }
  }

  /**
   * Train a reranker, based on external training data.
   */
  def apply(training_data: Iterable[ExtInst]) = {
    val rerank_classifier = train_rerank_classifier(training_data)
    val initial_ranker = create_initial_ranker(training_data)
    create_reranker(rerank_classifier, initial_ranker)
  }
}
