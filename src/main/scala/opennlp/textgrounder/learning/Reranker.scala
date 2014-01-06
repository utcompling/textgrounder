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

import util.debug._
import util.io.localfh
import util.math.standardize
import util.metering._
import util.print.errprint

import scala.util.Random

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
  def initial_ranker: Ranker[Query, Candidate]

  /**
   * Rerank the given candidates, based on an initial ranking.
   * FIXME: Should be `protected` but can't due to use of composition in
   * `gridlocate` (`GridReranker`).
   */
  def rerank_candidates(item: Query,
    initial_ranking: Iterable[(Candidate, Double)], correct: Candidate
  ): Iterable[(Candidate, Double)]

  def evaluate_with_initial_ranking(item: Query, correct: Candidate,
      include_correct: Boolean) = {
    // Do initial ranking.
    val initial_ranking = initial_ranker.evaluate(item, correct,
      include_correct)
    // Split into candidates to rerank and others.
    val (to_rerank, others) = initial_ranking.splitAt(top_n)
    val rescaled_to_rerank = if (debug("rescale")) {
      // FIXME! We also rescale the scores afterwards in
      // rescale_featvecs.
      // Rescale the initial scores so they are comparable across
      // different instances.
      val (cands, scores) = to_rerank.unzip
      cands zip standardize(scores)
    } else to_rerank
    // Rerank the candidates.
    val reranked = rerank_candidates(item, rescaled_to_rerank, correct)
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

  override def imp_evaluate(item: Query, correct: Candidate,
      include_correct: Boolean) = {
    val (initial_ranking, reranking) =
      evaluate_with_initial_ranking(item, correct, include_correct)
    reranking
  }
}

/**
 * A reranker that picks randomly among the candidates to rerank.
 */
class RandomReranker[Query, Candidate](
  val initial_ranker: Ranker[Query, Candidate],
  val top_n: Int
) extends Reranker[Query, Candidate] {
  /**
   * Rerank a set of top candidates, given the query and the initial score
   * for each candidate.
   */
  def rerank_candidates(item: Query,
      scored_candidates: Iterable[(Candidate, Double)], correct: Candidate) = {
    (new Random()).shuffle(scored_candidates)
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
 *
 * @param initial_ranker Ranker used to generate initial ranking.
 * @param top_n Number of top candidates from initial ranker to rerank.
 * @param rerank_classifier Scoring classifier for use in reranking.
 */
abstract class PointwiseClassifyingReranker[Query, Candidate](
  val initial_ranker: Ranker[Query, Candidate],
  val top_n: Int,
  rerank_classifier: ScoringClassifier
) extends Reranker[Query, Candidate] {
  // FIXME! This is really ugly.
  val write_test_data_file = {
    val outfile = debugval("write-rerank-test-data")
    if (outfile != "") {
      errprint(s"Writing rerank test data to file: $outfile")
      val file = localfh.openw(outfile)
      scala.sys.addShutdownHook(file.close)
      Some(file)
    } else
      None
  }

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
  def rerank_candidates(item: Query,
      scored_candidates: Iterable[(Candidate, Double)], correct: Candidate) = {
    // For each candidate, compute a feature vector.
    val cand_featvecs =
      for (((candidate, score), rank) <- scored_candidates.zipWithIndex)
        yield create_candidate_eval_featvec(item, candidate, score, rank)
    // Combine into an aggregate feature vector.
    val agg = new AggregateFeatureVector(cand_featvecs.toArray)
    if (write_test_data_file != None) {
      val maybe_label = scored_candidates.view.zipWithIndex.find {
        case ((cand, score), index) => cand == correct
      }
      if (maybe_label != None) {
        val (_, the_label) = maybe_label.get
        TrainingData.export_aggregate_to_file(write_test_data_file.get,
          agg, the_label)
      }
    }
    // Rescore component vectors using classifier.
    val new_scores0 = rerank_classifier.score(agg).toIndexedSeq
    val new_scores =
      if (debug("negate-scores"))
        new_scores0.map(-_)
      else
        new_scores0
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
 *    This also consists of ExtInst items and is taken from the external
 *    training data by sectioning it into splits, where the data from each
 *    split is ranked on the training data from the remaining splits.
 * -- Query training data in the form of query training instances (QTI's),
 *    directly encapsulating a query, the candidate list scored by the initial
 *    ranker, and correct candidate. This is the raw-form data needed to
 *    generate the feature vectors that are fed to the reranker.
 * -- Feature training data is the data actually used to train a reranker.
 *    Each item is a "feature training instance" (FTI, of generic type
 *    `FTI`, which must be a subclass of `DataInstance`), encapsulating an
 *    aggregate feature vector of type `AggregateFeatureVector` and the
 *    label of the correct candidate. The aggregate feature vector in turn
 *    encapsulates the set of individual feature vectors, one per candidate,
 *    consisting of features specifying the compatibility between query and
 *    candidate. See `PointwiseClassifyingReranker` for more details.
 *
 * The use of abstract types, rather than tuples, allows the application to
 * choose how to encapsulate the data (e.g. whether to use a raw form or
 * precompute the necessary object) and to encapsulate other information,
 * if necessary.
 *
 * The handling of `ExtInst` allows external instances to consist of
 * raw-form documents (e.g. direct descriptions of the words in the document),
 * where the query object in a QTI is a "cooked" form of the document that
 * requires global information computed from the other splits of the external
 * data (e.g. back-off statistics in a language model). This is used by
 * `GridLocate`, where `ExtInst` is `DocStatus[Row]`, describing the raw-data
 * form of a document read from an external corpus. Generating an initial
 * ranker computes back-off statistics from the set of initial-ranker training
 * data used to initialize the ranker, and in turn, conversion of a raw
 * document to query-candidate pairs that underlie a QTI (using
 * `external_instances_to_query_candidate_pairs`) makes use of these back-off
 * statistics to generate the "cooked" query document of type `GridDoc[Co]`.
 * In other words, the same `ExtInst` object might generate different
 * query-candidate pairs depending on which split it falls in and what the
 * nature of the external instances in the remaining splits is.
 *
 *    gener correctinformation necessary to generate a feature training
 *    (QTI), encapsulating information necessary to generate a feature training
 *    instance (FTI), directly usable by a classifier trainer (see above).
 *    The QTI consists of the query itself, the candidate list with associated
 *    scores for each candidate when ranked using the initial ranker, and the
 *    identity of the correct candidate, which must be in the candidate list.
 *    The candidate list in the QTI is determined by taking the top-ranked N
 *    candidates (N = `top_n`) from the ranking produced by the initial
 *    ranker, and augmenting if necessary with the correct candidate.
 *    We take one other candidate away when doing this; although it's not
 *    always necessary (a single-weight classifier in general can handle
 *    a variable number of classes since it treats them all the same),
 *    it's required for some classifiers (e.g. those based on R's mlogit()
 *    function).
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
 * 4. For each external instance, generate a query training instance
 *    (QTI); see above:
 *
 *    -- The query object in the QTI is a cooked form of the document
 *       contained in the external instance;
 *    -- the correct candidate is computed from the dependent variable
 *       in the external instance (in GridLocate, candidates are cells,
 *       and the correct cell is determined from the document's coordinate
 *       by finding the cell containing the coordinate);
 *    -- the candidate list is determined by taking the top-ranked N
 *       candidates (N = `top_n`) from the ranking produced by the initial
 *       ranker, and augmenting if necessary with the correct candidate.
 *       We take one other candidate away when doing this; although it's not
 *       always necessary (a single-weight classifier in general can handle
 *       a variable number of classes since it treats them all the same,
 *       and in fact TADM can handle this without problem), it's required
 *       for some classifiers (e.g. those based on R's mlogit() function).
 *
 * 5. Once the entire set of QTI's is generated for all slices, convert each
 *    QTI into an FTI (see above). The FTI describes the same data as the
 *    QTI but in the form of an aggregate feature vector (grouping individual
 *    feature vectors for each candidate), which can be directly used by the
 *    underlying classifier trainer. The reason for first generating a QTI,
 *    rather than directly generating an FTI, is that it may be necessary to
 *    use some global properties of the training corpus to generate the FTI,
 *    which cannot be determined until the entire corpus is processed.
 *    (In particular, we don't know the length of the feature vectors until
 *    we've counted up the distinct feature types that occur anywhere in our
 *    training data.  All feature vectors must, conceptually, have the same
 *    length, even if the data itself is stored sparsely.)
 *
 * @tparam Query type of a query
 * @tparam Candidate type of a possible candidate
 * @tparam ExtInst type of the instances used in external training data
 * @tparam FTI type of the feature training instances (the final-form
 *   instances directly usable by the reranker, consisting of feature vectors)
 */
trait PointwiseClassifyingRerankerTrainer[
  Query, Candidate, ExtInst, FTI <: DataInstance
] extends RerankerLike[Query, Candidate] { self =>

  /**
   * A class encapsulating a rerank data instance in raw form, used to
   * generate the feature vectors that are actually fed to the ranker.
   * A QTI is associated with an external instance from the training
   * corpus and consists of a query object, the candidate list scored by
   * the initial ranker, and the correct candidate, which must be in
   * the candidate list. See `PointwiseClassifyingRerankerTrainer`.
   */
  case class QueryTrainingInst(query: Query, correct: Candidate,
      cand_scores: Iterable[(Candidate, Double)]) {
    /**
     * Return the label (zero-based index) of correct candidate among the
     * candidate list.  Assertion failure if not found, since correct candidate
     * should always be in candidate list.
     */
    def label: LabelIndex = {
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
     * FTI (see `PointwiseClassifyingRerankerTrainer`).
     *
     * @param create_candidate_featvec Function converting a
     *    query-candidate pair, plus score from the initial ranker, into a
     *    feature vector.
     */
    def aggregate_featvec(
      create_candidate_featvec: (Query, Candidate, Double, Int) =>
        FeatureVector
    ) = {
      // Get sequence of feature vectors
      val featvecs =
        (for (((cand, score), rank) <- cand_scores.zipWithIndex) yield
          create_candidate_featvec(query, cand, score, rank)).toArray
      val agg = new AggregateFeatureVector(featvecs)
      if (debug("rescale"))
        agg.rescale_featvecs()
      agg
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

  /**
   * Create the classifier used for reranking, given training data.
   * Each data instance describes a training document in a form
   * directly usable by the classifier trainer: A rerank training instance
   * (FTI, see above) and an integer specifying the label of the correct
   * candidate (corresponding to an index into the FTI's aggregate feature
   * vector).
   */
  protected def create_rerank_classifier(data: TrainingData[FTI]
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
   * Convert a `QueryTrainingInst` object to an `FTI` object, i.e. from
   *
   * encapsulating info used to training the rerank classifier.
   */
  protected def query_training_data_to_feature_vectors(
    data: Iterable[QueryTrainingInst]
  ): Iterable[(FTI, LabelIndex)]

  /**
   * Display a query item (typically for debugging purposes).
   */
  def format_query_item(item: Query) = item.toString

  /**
   * Display an candidate (typically for debugging purposes).
   */
  def format_candidate(candidate: Candidate) = candidate.toString

  /**
   * Generate a query training instance. See `QueryTrainingInst` above.
   *
   * @param query Query object corresponding to a training document.
   * @param correct Correct candidate, as determined from the training corpus.
   * @param initial_ranker Initial ranker used to score the candidates.
   */
  protected def get_query_training_instance(
    query: Query, correct: Candidate, initial_ranker: Ranker[Query, Candidate]
  ): QueryTrainingInst = {
    val initial_candidates =
      initial_ranker.evaluate(query, correct, include_correct = true)
    val top_candidates = initial_candidates.take(top_n)
    val cand_scores =
      if (top_candidates.find(_._1 == correct) != None)
        top_candidates
      else
        // Augment with correct candidate, but take one of the other
        // candidates away so we have the same number.
        top_candidates.take(top_n - 1) ++
          Iterable(initial_candidates.find(_._1 == correct).get)
    val rescaled_cand_scores = if (debug("rescale")) {
      // Rescale the initial scores so they are comparable across
      // different instances.
      val (cands, scores) = cand_scores.unzip
      cands zip standardize(scores)
    } else
      cand_scores

    QueryTrainingInst(query, correct, rescaled_cand_scores)
  }

  /**
   * Given one of the splits of external training instances and an initial
   * ranker created from the remainder, generate the corresponding query
   * training instances. See `QueryTrainingInst`.
   */
  protected def get_query_training_data_for_split(
      splitnum: Int, data: Iterable[ExtInst],
      initial_ranker: Ranker[Query, Candidate]
    ): Iterator[QueryTrainingInst] = {
    val query_candidate_pairs =
      external_instances_to_query_candidate_pairs(data.iterator, initial_ranker)
    val task = new Meter("generating",
      s"split-#${splitnum + 1} rerank training instance")
    if (debug("rerank-training")) {
      query_candidate_pairs.zipWithIndex.mapMetered(task) {
        case ((query, correct), index) => {
          val prefix = s"#${index + 1}: "
          val query_training_data =
            get_query_training_instance(query, correct, initial_ranker)
          query_training_data.debug_out(prefix)
          query_training_data
        }
      }
    } else {
      query_candidate_pairs.mapMetered(task) {
        case (query, correct) =>
          get_query_training_instance(query, correct, initial_ranker)
      }
    }
  }

  /**
   * Given all external training instances, generate all corresponding
   * query training instances. See `QueryTrainingInst`. This involves
   * splitting up the rerank data into parts and training
   * separate initial rankers, as described above.
   */
  protected def get_query_training_data(training_data: Iterable[ExtInst]
  ): Iterable[QueryTrainingInst] = {
    val numitems = training_data.size
    // If number of docs is not an even multiple of number of splits,
    // round the split size up -- we want the last split a bit smaller
    // rather than an extra split with only a couple of items.
    val splitsize = (numitems + number_of_splits - 1) / number_of_splits
    errprint(s"#training docs = $numitems ($number_of_splits splits, $splitsize docs/split)")
    (0 until number_of_splits) flatMap { rerank_split_num =>
      errprint(
        s"======== Generating data for split ${rerank_split_num + 1} ========")
      val split_training_data = training_data.grouped(splitsize).zipWithIndex
      val (rerank_splits, initrank_splits) = split_training_data partition {
        case (data, num) => num == rerank_split_num
      }
      val rerank_data = rerank_splits.flatMap(_._1).toIndexedSeq
      val initrank_data = initrank_splits.flatMap(_._1).toIndexedSeq
      val split_initial_ranker = create_initial_ranker(initrank_data)
      val split_qtd = get_query_training_data_for_split(rerank_split_num,
        rerank_data, split_initial_ranker).toIndexedSeq
      // Skipping will happen e.g. in GridLocate when there are no
      // training documents in the external instance's cell -- remember
      // that the training documents in question exclude the slice containing
      // the external instance.
      //
      // FIXME! Is this necessary and if so do we want to skip other
      // external documents with very few training documents in the cell?
      errprint(s"Split #${rerank_split_num + 1}: size = ${split_qtd.size}" +
        s" (${rerank_data.size - split_qtd.size} skipped)")
      split_qtd
    }
  }

  /**
   * Train a rerank classifier, based on external training data.
   */
  protected def train_rerank_classifier(
    ext_training_data: Iterable[ExtInst]
  ) = {
    val query_training_data = get_query_training_data(ext_training_data)
    val aggregate_fvs =
      query_training_data_to_feature_vectors(query_training_data)
    val feature_training_data = TrainingData(aggregate_fvs)
    val training_data_file = debugval("write-rerank-training-data")
    if (training_data_file != "")
      feature_training_data.export_to_file(training_data_file)
    create_rerank_classifier(feature_training_data)
  }

  /**
   * Actually create a reranker object, given a rerank classifier and
   * initial ranker.
   */
  protected def create_reranker(
    rerank_classifier: ScoringClassifier,
    initial_ranker: Ranker[Query, Candidate]
  ): Reranker[Query, Candidate] = {
    new PointwiseClassifyingReranker[Query, Candidate](
        initial_ranker, self.top_n, rerank_classifier) {
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
