///////////////////////////////////////////////////////////////////////////////
//  Ranker.scala
//
//  Copyright (C) 2012-2013 Ben Wing, The University of Texas at Austin
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

import util.collection.is_reverse_sorted
import util.{math => umath}
import util.metering._
import util.print._
import learning._

import util.debug._

/**
 * A basic ranker. This is a machine-learning object that is given a query
 * item and a set of possible candidates and ranks the candidates by
 * determining a score for each one (on an arbitrary scale). The terminology
 * of queries and candidates from the "learning to rank" field within the
 * larger field of information retrieval. A paradigmatic example is a search
 * engine, where the query is the string typed into the search engine, the
 * candidates are a set of possibly relevant documents, and the result of
 * ranking should be an ordered list of the documents, from most to least
 * relevant.
 *
 * For the GridLocate application, a query is a document, a candidate is a
 * cell, and the ranker determines which cells are most likely to be the
 * correct ones.
 *
 * The objective function used for typical search-engine ranking is rather
 * different from what is used in GridLocate. In the former case we care
 * about all the candidates (potentially relevant documents) near the top of
 * the ranked list, where in the latter case we only really care about the
 * top-ranked candidate, i.e. cell (and moreover, we normally score an
 * erroneous top-ranked cell not simply by the fact that it is wrong but
 * how wrong it is, i.e. the distance between the cell's centroid location
 * and the document's actual location).
 */
trait Ranker[Query, Candidate] {
  /**
   * Evaluate a query item, returning a list of ranked candidates from best to
   * worst, with a score for each.  The score must not increase from any
   * candidate to the next one.  Any candidates mentioned in `include` must be
   * included in the returned list.
   */
  def imp_evaluate(item: Query, include: Iterable[Candidate]):
    Iterable[(Candidate, Double)]

  final def evaluate(item: Query, include: Iterable[Candidate]) = {
    val scored_cands = imp_evaluate(item, include)
    assert(is_reverse_sorted(scored_cands.map(_._2)))
    scored_cands
  }
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
trait Reranker[Query, Candidate]
    extends Ranker[Query, Candidate]
    with RerankerLike[Query, Candidate] {
  /** Ranker for generating initial ranking. */
  protected def initial_ranker: Ranker[Query, Candidate]

  /**
   * Rerank the given candidates, based on an initial ranking.
   */
  protected def rerank_candidates(item: Query,
    initial_ranking: Iterable[(Candidate, Double)]): Iterable[(Candidate, Double)]

  def evaluate_with_initial_ranking(item: Query,
      include: Iterable[Candidate]) = {
    // Do initial ranking.
    val initial_ranking = initial_ranker.evaluate(item, include)
    // Split into candidates to rerank and others.
    val (to_rerank, others) = initial_ranking.splitAt(top_n)
    // Standardize the initial scores so they are comparable across
    // different instances.
    val (cands, scores) = to_rerank.unzip
    val std_to_rerank = cands zip umath.standardize(scores)
    // Rerank the candidates.
    val reranked = rerank_candidates(item, std_to_rerank)
    // Adjust the scores of the reranked candidates to be above all
    // the others.
    val min_rerank = reranked.map(_._2).min
    val max_others = others.map(_._2).max
    val adjust = -min_rerank + max_others + 1
    val adjusted_reranked = reranked.map { case (cand, score) =>
      (cand, score + adjust) }
    val reranking = adjusted_reranked ++ others
    (initial_ranking, reranking)
  }

  override def imp_evaluate(item: Query, include: Iterable[Candidate]) = {
    val (initial_ranking, reranking) =
      evaluate_with_initial_ranking(item, include)
    reranking
  }
}

/**
 * A pointwise classifying reranker. This is a "reranker" in that it uses
 * an initial ranker to construct a ranking over the entire set of candidates,
 * using a fairly basic mechanism, then takes the top N candidates for some N
 * (e.g. 10 or 50), and uses a more expensive but hopefully more accurate
 * mechanism to rescore them. It is "pointwise" in that each candidate is
 * scored independently of the others (rather than, e.g., scoring pairs of
 * candidates against each other), and it is a "classifying reranker" in that
 * it uses the mechanism of a scoring classifier to assign a score to each
 * possible candidate to be reranked.
 *
 * For each possible candidate, we construct a "query-candidate pair" (simply a
 * grouping of the query and particular candidate) and from it we create
 * a "candidate feature vector" combining individual features describing
 * the compatibility between query and candidate.  The feature vectors for
 * all N candidates get grouped into an "aggregate feature vector"
 * (of type `AggregateFeatureVector`), which is then passed to the
 * classifier to be scored.
 *
 * @tparam Query type of a query
 * @tparam Candidate type of a possible candidate
 */
trait PointwiseClassifyingReranker[Query, Candidate]
    extends Reranker[Query, Candidate] {
  /** Scoring classifier for use in reranking. */
  protected def rerank_classifier: ScoringClassifier

  val feature_mapper: FeatureMapper
  val label_mapper: LabelMapper

  /**
   * Create a candidate feature vector to feed to the classifier during
   * evaluation, given a query item, a potential candidate from the
   * ranker, and the score from the initial ranker on this candidate.
   */
  protected def create_candidate_eval_featvec(query: Query,
    candidate: Candidate, initial_score: Double, initial_rank: Int
  ): FeatureVector

  /**
   * Rerank a set of top candidates, given the query and the initial score
   * for each candidate.
   */
  protected def rerank_candidates(item: Query,
      scored_candidates: Iterable[(Candidate, Double)]) = {
    val cand_featvecs =
      for (((candidate, score), rank) <- scored_candidates.zipWithIndex)
        yield create_candidate_eval_featvec(item, candidate, score, rank)
    val query_featvec =
      new AggregateFeatureVector(cand_featvecs.toIndexedSeq, feature_mapper,
        label_mapper)
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
 *    corpus), consisting of items of generic type `ExtInst`, encapsulating
 *    a query object (of type `Query`) and the correct candidate (of type
 *    `Candidate`).
 * -- Initial-ranker training data is the data used to train an initial
 *    ranker, which supplies the base ranking upon which the reranker builds.
 *    This also consists of ExtInst items and is taken directly from the
 *    external training data or a subset.
 * -- Reranker training data is the data used to train a rerank classifier,
 *    i.e. the classifier used to compute the scores that underlie the
 *    reranking. Each item is a "rerank training instance" (RTI, of type
 *    `RTI`), encapsulating an aggregate feature vector of type
 *    `AggregateFeatureVector` and the label of the correct candidate.
 *    The aggregate feature vector in turn encapsulates the set of
 *    individual feature vectors, one per candidate, consisting of
 *    features specifying the compatibility between query and candidate.
 *    See `PointwiseClassifyingReranker` for more details.
 *
 * The use of abstract types, rather than tuples, allows the application to
 * choose how to encapsulate the data (e.g. whether to use a raw form or
 * precompute the necessary object) and to encapsulate other information,
 * if necessary.
 *
 * `ExtInst` in particular is handled in such a way that it can encapsulate
 * raw-form information, where conversion to query-candidate pairs that
 * underlie the reranker training data might depend on global properties
 * computed from the subset of data used to create an initial ranker.
 *
 * `GridLocate` requires this facility, making `ExtInst` be the type
 * `DocStatus[Row]`, describing the raw-data form of a document read
 * from an external corpus. Generating an initial ranker computes back-off
 * statistics from the set of initial-ranker training data used to initialize
 * the ranker, and in turn, conversion of a raw document to a query-candidate
 * pair (using `external_instances_to_query_candidate_pairs`) makes use of
 * these back-off statistics to generate the "cooked" query object of type
 * `GridDoc[Co]`. In other words, the same `ExtInst` object might generate
 * multiple query-candidate pairs depending on which split it falls in and
 * whether this split is used for training the initial ranker of the reranker.
 *
 * Training is as follows:
 *
 * 1. External training data (a set of "external instances") is supplied
 *    by the caller.
 *
 * 2. Split this data into N parts ("splits" or "slices", the number
 *    determined by `number_of_splits`), and process each slice in turn.
 *
 * 3. For a given slice, train an initial ranker based on the remaining
 *    slices. Then process each external instance in the slice.
 *
 * 4. For each external instance, generate a "query training data" object
 *    (QTD), encapsulating information necessary to generate a rerank training
 *    instance (RTI), directly usable by a classifier trainer (see above).
 *    The QTD consists of the query itself, the candidate list with associated
 *    scores for each candidate when ranked using the initial ranker, and the
 *    identity of the correct candidate, which must be in the candidate list.
 *    The candidate list in the QTD is determined by taking the top-ranked N
 *    candidates (N = `top_n`) from the ranking produced by the initial
 *    ranker, and augmenting if necessary with the correct candidate.
 *    We take one other candidate away when doing this; although it's not
 *    always necessary (a single-weight classifier in general can handle
 *    a variable number of classes since it treats them all the same),
 *    it's required for some classifiers (e.g. those based on R's mlogit()
 *    function).
 *
 * 5. Once the entire set of QTD's is generated for all slices, convert each
 *    QTD into an RTI (see above). The RTI describes the same data as the
 *    QTD but in the form of an aggregate feature vector (grouping individual
 *    feature vectors for each candidate), which can be directly used by the
 *    underlying classifier trainer. The reason for first generating a QTD,
 *    rather than directly generating an RTI, is that it may be necessary to
 *    use some global properties of the training corpus to generate the RTI,
 *    which cannot be determined until the entire corpus is processed.
 *    (In particular, we don't know the length of the feature vectors until
 *    we've counted up the distinct feature types that occur anywhere in our
 *    training data.  All feature vectors must, conceptually, have the same
 *    length, even if the data itself is stored sparsely.)
 *
 * @tparam Query type of a query
 * @tparam Candidate type of a possible candidate
 * @tparam ExtInst type of the instances used in external training data
 * @tparam RTI type of the instances used in external training data
 */
trait PointwiseClassifyingRerankerTrainer[
  Query, Candidate, ExtInst, RTI <: DataInstance
] extends RerankerLike[Query, Candidate] { self =>

  /**
   * A class encapsulating "query training data" (QTD) information necessary
   * to generate a rerank training instance (RTI). A QTD is associated with
   * an external instance from the training corpus and contains, in a fairly
   * raw form, the query itself, the candidate list with associated scores
   * for each candidate when ranked using the initial ranker, and the identity
   * of the correct candidate, which must be in the candidate list. See
   * `PointwiseClassifyingRerankerTrainer`.
   */
  case class QueryTrainingData(query: Query, correct: Candidate,
      cand_scores: Iterable[(Candidate, Double)]) {
    /**
     * Return the label (zero-based index) of correct candidate among the
     * candidate list.  Assertion failure if not found, since correct candidate
     * should always be in candidate list.
     */
    def label: Int = {
      val maybe_label = cand_scores.zipWithIndex.find {
        case ((cand, featvec), index) => cand == correct
      }
      assert(maybe_label != None,
        "Correct candidate should be in candidate list")
      val (_, the_label) = maybe_label.get
      the_label
    }

    /**
     * Convert the data in the object into an aggregate feature vector
     * describing the candidates for a query. This is the primary data in an
     * RTI (see `PointwiseClassifyingRerankerTrainer`).
     *
     * @param create_candidate_featvec Function converting a
     *    query-candidate pair, plus score from the initial ranker, into a
     *    feature vector.
     */
    def aggregate_featvec(
      create_candidate_featvec: (Query, Candidate, Double, Int) =>
        FeatureVector
    ) = {
      val featvecs =
        for (((cand, score), rank) <- cand_scores.zipWithIndex) yield
          create_candidate_featvec(query, cand, score, rank)
      new AggregateFeatureVector(featvecs.toIndexedSeq, feature_mapper,
        label_mapper)
    }

    /**
     * Describe this object for debugging purposes.
     */
    def debug_out(prefix: String) {
      errprint("%sTraining item: %s", prefix, format_query_item(query))
      errprint("%sTrue candidate: %s", prefix, format_candidate(correct))
      for (((candidate, score), candind) <- cand_scores.zipWithIndex) {
        val instpref = "%s#%s: " format (prefix, candind)
        val correctstr =
          if (correct == candidate) "CORRECT" else "INCORRECT"
        errprint("%s%s: (score %s) %s", instpref, correctstr, score, candidate)
      }
    }
  }

  /**
   * Number of splits used in the training data, to create the reranker.
   */
  val number_of_splits: Int

  /** Feature mapper used to memoize features. */
  val feature_mapper: FeatureMapper
  /** Label mapper used to memoize labels, or simulated. */
  val label_mapper: LabelMapper

  /**
   * Create the classifier used for reranking, given training data.
   * Each data instance describes a training document in a form
   * directly usable by the classifier trainer: A rerank training instance
   * (RTI, see above) and an integer specifying the label of the correct
   * candidate (corresponding to an index into the RTI's aggregate feature
   * vector).
   */
  protected def create_rerank_classifier(data: Iterable[(RTI, Int)]
  ): ScoringClassifier

  /**
   * Create a candidate feature vector (see above) to feed to
   * the classifier during evaluation, given a query item, a potential
   * candidate from the ranker, and the score from the initial ranker on this
   * candidate.  Note that this function may need to work differently from
   * the corresponding function used during training, typically in that new
   * features cannot be created (hence, e.g., when handling a previously
   * unseen word we need to skip it rather than creating a previously
   * unseen feature).
   */
  protected def create_candidate_eval_featvec(query: Query,
    candidate: Candidate, initial_score: Double, initial_rank: Int
  ): FeatureVector

  /**
   * Create an initial ranker based on a set of external instances.
   */
  protected def create_initial_ranker(data: Iterable[ExtInst]):
    Ranker[Query, Candidate]

  /**
   * Convert a set of external instances into query-candidate pairs.
   * Each external instance maps to a query object describing the data
   * in the instances, plus the correct candidate for the query.
   * We need the initial ranker passed in because the set of potential
   * candidates may not exist until the ranker is created (e.g. in
   * GridLocate, cells are populated based on the training data, and in
   * the case of K-d trees, the cells themselves are shaped from the
   * training data). Some external instances may not be convertible
   * (e.g. in GridLocate, if there are no training documents in the
   * external instance's cell -- remember that the training documents in
   * question exclude the slice containing the external instance).
   *
   * FIXME! There may be a problem with K-d trees since we generate
   * multiple initial rankers, one per slice, and hence there will be
   * different cell arrangements.
   *
   * @param inst Iterator over external instances in a given slice
   * @param initial_ranker initial ranker trained from the external
   *   instances in all other slices
   */
  protected def external_instances_to_query_candidate_pairs(
    inst: Iterator[ExtInst], initial_ranker: Ranker[Query, Candidate]
  ): Iterator[(Query, Candidate)]

  /**
   * Convert a `QueryTrainingData` object to an `RTI` object, i.e. from
   *
   * encapsulating info used to training the rerank classifier.
   */
  protected def query_training_data_to_rerank_training_instances(
    data: Iterable[QueryTrainingData]
  ): Iterable[(RTI, Int)]

  /**
   * Display a query item (typically for debugging purposes).
   */
  def format_query_item(item: Query) = item.toString

  /**
   * Display an candidate (typically for debugging purposes).
   */
  def format_candidate(candidate: Candidate) = candidate.toString

  /**
   * Generate a QTD (query training data) object. See `QueryTrainingData`
   * above.
   *
   * @param query Query object corresponding to a training document.
   * @param correct Correct candidate, as determined from the training corpus.
   * @param initial_ranker Initial ranker used to score the candidates.
   */
  protected def get_query_training_data(
    query: Query, correct: Candidate, initial_ranker: Ranker[Query, Candidate]
  ): QueryTrainingData = {
    val initial_candidates =
      initial_ranker.evaluate(query, Iterable(correct))
    val top_candidates = initial_candidates.take(top_n)
    val cand_scores =
      if (top_candidates.find(_._1 == correct) != None)
        top_candidates
      else
        // Augment with correct candidate, but take one of the other
        // candidates away so we have the same number.
        top_candidates.take(top_n - 1) ++
          Iterable(initial_candidates.find(_._1 == correct).get)
    val (cands, scores) = cand_scores.unzip
    val std_cand_scores = cands zip umath.standardize(scores)

    QueryTrainingData(query, correct, std_cand_scores)
  }

  /**
   * Given one of the splits of external training instances and an initial
   * ranker created from the remainder, generate the corresponding QTD
   * objects. See `QueryTrainingData`.
   */
  protected def get_rerank_training_data_for_split(
      splitnum: Int, data: Iterator[ExtInst],
      initial_ranker: Ranker[Query, Candidate]
    ): Iterator[QueryTrainingData] = {
    val query_candidate_pairs =
      external_instances_to_query_candidate_pairs(data, initial_ranker)
    val task = new Meter("generating",
      "split-#%s rerank training instance" format splitnum)
    if (debug("rerank-training")) {
      query_candidate_pairs.zipWithIndex.mapMetered(task) {
        case ((query, correct), index) => {
          val prefix = "#%s: " format (index + 1)
          val query_training_data =
            get_query_training_data(query, correct, initial_ranker)
          query_training_data.debug_out(prefix)
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
   * Given all external training instances, generate all corresponding QTD's
   * (query training data objects). See `QueryTrainingData`. This involves
   * splitting up the rerank data into parts and training
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
      val feature_mapper = self.feature_mapper
      val label_mapper = self.label_mapper
      protected def create_candidate_eval_featvec(query: Query,
          candidate: Candidate, initial_score: Double, initial_rank: Int) = {
        self.create_candidate_eval_featvec(query, candidate,
          initial_score, initial_rank)
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
