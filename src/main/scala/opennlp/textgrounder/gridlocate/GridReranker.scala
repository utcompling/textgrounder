///////////////////////////////////////////////////////////////////////////////
//  GridReranker.scala
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

import util.print._
import util.numeric.{pretty_long, pretty_double}
import util.textdb.Row

import learning._
import learning.vowpalwabbit._

/**
 * Object encapsulating a GridLocate data instance to be used by the
 * classifier that underlies the reranker. This corresponds to a document
 * in the training corpus and serves as the main part of an RTI (rerank
 * training instance, see `PointwiseClassifyingRerankerTrainer`).
 */
case class GridRerankerInst[Co](
  doc: GridDoc[Co],
  agg: AggregateFeatureVector,
  candidates: IndexedSeq[GridCell[Co]]
) extends GridRankerInst[Co] {
  def get_cell(index: LabelIndex) = candidates(index)
}

/**
 * A grid ranker that uses reranking.
 *
 * @tparam Co Type of document's identifying coordinate (e.g. a lat/long tuple,
 *   a year, etc.), which tends to determine the grid structure.
 * @param wrapped Underlying reranker
 * @param ranker_name Name of the ranker, for output purposes
 */
class GridReranker[Co](
  wrapped: Reranker[GridDoc[Co], GridCell[Co]],
  ranker_name: String
) extends GridRanker(ranker_name + "/" +
    wrapped.initial_ranker.asInstanceOf[GridRanker[Co]].ranker_name,
  wrapped.initial_ranker.asInstanceOf[GridRanker[Co]].grid
) with Reranker[GridDoc[Co], GridCell[Co]] {

  val top_n = wrapped.top_n
  def initial_ranker = wrapped.initial_ranker

  def rerank_candidates(item: GridDoc[Co],
      initial_ranking: Iterable[(GridCell[Co], Double)],
      correct: Option[GridCell[Co]]) =
    wrapped.rerank_candidates(item, initial_ranking, correct)
}

/**
 * A grid reranker using a linear classifier.  See `PointwiseGridReranker`.
 *
 * @param trainer Factory object for training a linear classifier used for
 *   pointwise reranking.
 */
abstract class LinearClassifierGridRerankerTrainer[Co](
  ranker_name: String,
  val trainer: SingleWeightLinearClassifierTrainer[GridRerankerInst[Co]]
) extends PointwiseClassifyingRerankerTrainer[
    GridDoc[Co], GridCell[Co], DocStatus[Row], GridRerankerInst[Co],
    FeatureVector, AggregateFeatureVector, TrainingData[GridRerankerInst[Co]]
    ] { self =>
  protected def create_rerank_classifier(
    training_data: TrainingData[GridRerankerInst[Co]]
  ) = {
    val data = training_data.data
    errprint("Training linear classifier ...")
    errprint("Number of training items (aggregate feature vectors): %s",
      pretty_long(data.size))
    val num_indiv_training_items =
      data.view.map(_._1.feature_vector.depth.toLong).sum
    errprint("Number of individual feature vectors in training items: %s",
      pretty_long(num_indiv_training_items))
    val num_total_feats =
      num_indiv_training_items * data.head._1.feature_vector.length
    val num_total_stored_feats =
      data.view.map(_._1.feature_vector.stored_entries.toLong).sum
    errprint("Total number of features in all training items: %s",
      pretty_long(num_total_feats))
    errprint("Avg number of features per training item: %s",
      pretty_double(num_total_feats.toDouble / data.size))
    errprint("Total number of stored features in all training items: %s",
      pretty_long(num_total_stored_feats))
    errprint("Avg number of stored features per training item: %s",
      pretty_double(num_total_stored_feats.toDouble / data.size))
    errprint("Space reduction using sparse vectors: %s times",
      pretty_double(num_total_feats.toDouble /
        num_total_stored_feats))
    trainer(training_data)
  }

  /**
   * Actually create a reranker object, given a rerank classifier and
   * initial ranker.
   */
  override protected def create_reranker(
    _rerank_classifier: ScoringClassifier[FeatureVector],
    _initial_ranker: Ranker[GridDoc[Co], GridCell[Co]]
  ) = {
    val reranker = super.create_reranker(_rerank_classifier, _initial_ranker)
    new GridReranker(reranker, ranker_name)
  }

  /**
   * Train a reranker, based on external training data.
   */
  override def apply(training_data: Iterable[DocStatus[Row]]) =
    super.apply(training_data).asInstanceOf[GridReranker[Co]]

  override def format_query_item(item: GridDoc[Co]) = {
    "%s, lm=%s" format (item, item.rerank_lm.debug_string)
  }
}

class VowpalWabbitLabelDependentScoringClassifier(
  cfier: VowpalWabbitDaemonClassifier
) extends ScoringClassifier[RawFeatureVector] {
  def classify(inst: RawFeatureVector) = {
    score(inst).zipWithIndex.maxBy(_._1)._2
  }
  def number_of_labels(inst: RawFeatureVector) =
    inst.asInstanceOf[RawAggregateFeatureVector].data.size
  def pretty_print_labeled(inst: RawFeatureVector, prefix: String,
    correct: LabelIndex) = ???
  def score(inst: RawFeatureVector): IndexedSeq[Double] = {
    val rafv = inst.asInstanceOf[RawAggregateFeatureVector]
    val input =
      cfier.externalize_label_dependent_data_instance(rafv.data, correct = 0)
    val results =
      cfier(input, label_dependent = true)
    val sorted_results = results.head.toIndexedSeq.sortWith(_._1 < _._1)
    assert(sorted_results.size == rafv.data.size)
    sorted_results.map(_._2)
  }
}

/**
 * A grid reranker using VowpalWabbit.  See `PointwiseGridReranker`.
 *
 * @param trainer Factory object for training a VowpalWabbit daemon
 *   classifier.
 */
abstract class VowpalWabbitGridRerankerTrainer[Co](
  ranker_name: String,
  val trainer: VowpalWabbitDaemonTrainer,
  val vw_args: String
) extends PointwiseClassifyingRerankerTrainer[
    GridDoc[Co], GridCell[Co], DocStatus[Row],
    RawAggregateFeatureVector,
    RawFeatureVector,
    RawAggregateFeatureVector,
    RawAggregateTrainingData
    ] { self =>
  protected def create_rerank_classifier(
    training_data: RawAggregateTrainingData
  ) = {
    val data = training_data.data
    errprint("Training Vowpal Wabbit reranker classifier ...")
    errprint("Number of training items (aggregate feature vectors): %s",
      pretty_long(data.size))
//    val num_indiv_training_items =
//      data.view.map(_._1.feature_vector.depth.toLong).sum
//    errprint("Number of individual feature vectors in training items: %s",
//      pretty_long(num_indiv_training_items))
//    val num_total_feats =
//      num_indiv_training_items * data.head._1.feature_vector.length
//    val num_total_stored_feats =
//      data.view.map(_._1.feature_vector.stored_entries.toLong).sum
//    errprint("Total number of features in all training items: %s",
//      pretty_long(num_total_feats))
//    errprint("Avg number of features per training item: %s",
//      pretty_double(num_total_feats.toDouble / data.size))
//    errprint("Total number of stored features in all training items: %s",
//      pretty_long(num_total_stored_feats))
//    errprint("Avg number of stored features per training item: %s",
//      pretty_double(num_total_stored_feats.toDouble / data.size))
//    errprint("Space reduction using sparse vectors: %s times",
//      pretty_double(num_total_feats.toDouble /
//        num_total_stored_feats))

    val featsfile =
      trainer.write_label_dependent_feature_file(data.toIterator.map {
        case (rafv, correct) => (rafv.data, correct)
      })
    val cfier = trainer(featsfile, vw_args, Seq("--csoaa_ldf", "mc"))
    new VowpalWabbitLabelDependentScoringClassifier(cfier)
  }

  /**
   * Actually create a reranker object, given a rerank classifier and
   * initial ranker.
   */
  override protected def create_reranker(
    _rerank_classifier: ScoringClassifier[RawFeatureVector],
    _initial_ranker: Ranker[GridDoc[Co], GridCell[Co]]
  ) = {
    val reranker = super.create_reranker(_rerank_classifier, _initial_ranker)
    new GridReranker(reranker, ranker_name)
  }

  /**
   * Train a reranker, based on external training data.
   */
  override def apply(training_data: Iterable[DocStatus[Row]]) =
    super.apply(training_data).asInstanceOf[GridReranker[Co]]

  override def format_query_item(item: GridDoc[Co]) = {
    "%s, lm=%s" format (item, item.rerank_lm.debug_string)
  }
}
