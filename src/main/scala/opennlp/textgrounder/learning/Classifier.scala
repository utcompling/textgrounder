///////////////////////////////////////////////////////////////////////////////
//  Classifier.scala
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

/**
 * Code for machine-learning classification.
 *
 * @author Ben Wing
 */

import collection.mutable
import io.Source

import util.math.argmax
import util.metering._
import util.print._

import util.debug._

/**
 * Mix-in for classifiers and classifier trainers.
 *
 * @tparam DI Type of object used to describe a data instance.
 *   For classifiers themselves, this is a `FeatureVector`.  For classifier
 *   trainers, this encapsulates a feature vector and possibly other
 *   application-specific data.
 */
trait ClassifierLike[DI <: DataInstance] {
  /** Return number of labels associated with a given instance. (In
    * fixed-depth classifiers, this value is the same for all instances.) */
  def number_of_labels(inst: DI): Int
}

trait DataInstance {
  /** Get the feature vector corresponding to a data instance. */
  def feature_vector: FeatureVector
}

/**
 * A basic classifier.
 *
 * The code below is written rather generally to support cases where the
 * features in a feature vector may or may not vary depending on the label
 * to be predicted; where there may or may not be a separate weight vector
 * for each label; and where the number of labels to be predicted may or
 * may not vary from instance to instance.
 *
 * A basic distinction is made below between "fixed-depth" classifiers,
 * where the number of labels is fixed for all instances, and "variable-dpeth"
 * classifiers, where different instances may have different numbers of
 * labels.
 *
 * The two most common cases are:
 *
 * 1. A basic fixed-depth classifier. The number of labels is an overall
 *    property of the classifier. The feature vector describing an instance
 *    is a `SimpleFeatureVector`, i.e. it has the same features regardless
 *    of the label (and hence can be used with classifiers supporting any
 *    number of labels).  There is a separate weight vector for each label
 *    (using a `MultiVectorAggregate`) -- this is necessarily the case when
 *    the feature vectors themselves don't vary by label, since either the
 *    weights or features must vary from label to label or the classifier
 *    cannot distinguish one label from another.
 * 
 * 2. A basic variable-depth classifier.  Each instance has a particular
 *    number of allowable labels, which may vary from instance to instance.
 *    In such a case, since the number of possible labels isn't known in
 *    advance, there is a single weight vector for all labels (using a
 *    `SingleVectorAggregate`), and correspondingly the features in a given
 *    feature vector must vary from instance to instance (using an
 *    `AggregateFeatureVector`).
 */
trait Classifier extends ClassifierLike[FeatureVector] {
  /** Classify a given instance, returning the label (from 0 to
    * `number_of_labels`-1). */
  def classify(inst: FeatureVector): Int
}

trait ScoringClassifier extends Classifier {
  /** Score a given instance for a single label. */
  def score_label(inst: FeatureVector, label: Int): Double

  /** Return the best label. */
  def classify(inst: FeatureVector) =
    argmax(0 until number_of_labels(inst)) { score_label(inst, _) }

  /** Score a given instance.  Return a sequence of predicted scores, of
    * the same length as the number of labels present.  There is one score
    * per label, and the maximum score corresponds to the single predicted
    * label if such a prediction is desired. */
  def score(inst: FeatureVector): IndexedSeq[Double] =
    (0 until number_of_labels(inst)).map(score_label(inst, _)).toIndexedSeq

  /** Return a sorted sequence of pairs of `(label, score)`, sorted in
    * descending order.  The first item is the best label, hence the label
    * that will be returned by `classify` (except possibly when there are
    * multiple best labels).  The difference between the score of the first
    * and second labels is the margin between the predicted and best
    * non-predicted label, which can be viewed as a confidence measure.
    */
  def sorted_scores(inst: FeatureVector) = {
    val scores = score(inst)
    ((0 until scores.size) zip scores).sortWith(_._2 > _._2)
  }
}

abstract class LinearClassifier(
  val weights: VectorAggregate
) extends ScoringClassifier {
  def score_label(inst: FeatureVector, label: Int) =
    inst.dot_product(weights(label), label)

  /** Convert a feature index into a user-readable string. */
  def format_feature(index: Int) = index.toString

  /** Print the weight coefficients with the largest absolute value.
    *
    * @param num_items Number of items to print. If &lt;= 0, print all items.
    */
  def debug_print_weights(num_items: Int = -1) {
    errprint("Weights:")
    for (depth <- 0 until weights.depth) {
      val vec = weights(depth)
      errprint("  Weights at depth %s: ", depth)
      val all_sorted_items =
        vec.toIterable.toIndexedSeq.zipWithIndex.sortWith(_._1.abs > _._1.abs)
      val sorted_items =
        if (num_items <= 0) all_sorted_items
        else all_sorted_items.take(num_items)
      for (((coeff, feat), index) <- sorted_items.zipWithIndex) {
        errprint("#%s: %s (%s) = %s", index + 1, format_feature(feat),
          feat, coeff)
      }
    }
  }
}

/** Mix-in for a fixed-depth classifier (or trainer thereof). */
trait FixedDepthClassifierLike[DI <: DataInstance]
    extends ClassifierLike[DI] {
  val num_labels: Int
  def number_of_labels(inst: DI) = num_labels
}

/** Mix-in for a variable-depth classifier (or trainer thereof). */
trait VariableDepthClassifierLike[DI <: DataInstance]
    extends ClassifierLike[DI] {
  def number_of_labels(inst: DI) = inst.feature_vector.depth
}

/** Mix-in for a binary classifier (or trainer thereof). */
trait BinaryClassifierLike[DI <: DataInstance]
    extends FixedDepthClassifierLike[DI] {
  val num_labels = 2
}

class FixedDepthLinearClassifier(weights: VectorAggregate, val num_labels: Int)
    extends LinearClassifier(weights)
    with FixedDepthClassifierLike[FeatureVector] {
}

class VariableDepthLinearClassifier(weights: VectorAggregate)
    extends LinearClassifier(weights)
      with VariableDepthClassifierLike[FeatureVector] {
}

/**
 * A binary linear classifier, created from an array of weights.  Normally
 * created automatically by one of the trainer classes.
 */
class BinaryLinearClassifier (
  weights: SingleVectorAggregate
) extends LinearClassifier(weights)
     with BinaryClassifierLike[FeatureVector] {

  /** Classify a given instance, returning the label, either 0 or 1. */
  override def classify(inst: FeatureVector) = {
    val sc = binary_score(inst)
    if (sc > 0) 1 else 0
  }

  /** Score a given instance, returning a single real number.  If the score
    * is &gt; 0, 1 is predicted, else 0. */
  def binary_score(inst: FeatureVector) = inst.dot_product(weights(0), 1)

  override def score(inst: FeatureVector) =
      IndexedSeq(0, binary_score(inst))
}

/**
 * Class for training a classifier given a set of training instances and
 * associated labels.
 *
 * @tparam DI Type of data instance used in training the classifier.
 */
trait ClassifierTrainer[DI <: DataInstance]
    extends ClassifierLike[DI] {
}

/**
 * Class for training a linear classifier given a set of training instances and
 * associated labels.
 *
 * @tparam DI Type of data instance used in training the classifier.
 */
trait LinearClassifierTrainer[DI <: DataInstance]
    extends ClassifierTrainer[DI] {
  val factory: VectorAggregateFactory

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(data: Iterable[(DI, Int)]) = {
    val len = check_sequence_lengths(data)
    val weights = new_weights(len)
    for ((inst, label) <- data) {
      assert(label >= 0 && label <=
        (weights.max_label min inst.feature_vector.max_label))
    } 
    weights
  }

  /** Create and initialize a vector of weights of length `len`.
    * By default, initialized to all 0's, but could be changed. */
  def new_weights(len: Int) = new_zero_weights(len)

  /** Create and initialize a vector of weights of length `len` to all 0's. */
  def new_zero_weights(len: Int) = {
    errprint("Length of weight vector: %s", len)
    factory.empty(len)
  }

  /** Check that all instances have the same length, and return it. */
  def check_sequence_lengths(data: Iterable[(DI, Int)]) =
    FeatureVector.check_same_length(data.map(d => d._1.feature_vector))

  /** Iterate over a function to train a linear classifier.
    * @param error_threshold Maximum allowable error
    * @param max_iterations Maximum allowable iterations
    *
    * @param fun Function to iterate over; Takes a int (iteration number),
    *   returns a tuple (num_errors, num_adjustments, total_adjustment)
    *   where `num_errors` is the number of errors made on the training
    *   data and `total_adjustment` is the total sum of the scaling factors
    *   used to update the weights when a mistake is made. */
  def iterate(error_threshold: Double, max_iterations: Int)(
      fun: Int => (Int, Int, Double)) = {
    val task = new Meter("running", "classifier training iteration")
    task.start()
    var iter = 0
    var total_adjustment = 0.0
    do {
      iter += 1
      val (num_errors, num_adjustments, total_adjustment2) = fun(iter)
      total_adjustment = total_adjustment2 // YUCK!
      errprint("Iteration %s, num errors %s, num adjustments %s, total_adjustment %s",
        iter, num_errors, num_adjustments, total_adjustment)
      task.item_processed()
    } while (iter < max_iterations && total_adjustment >= error_threshold)
    task.finish()
    iter
  }

  /** Iterate over a function to train a linear classifier,
    * optionally averaging over the weight vectors at each iteration.
    * @param weights DOCUMENT ME
    * @param averaged If true, average the weights at each iteration
    * @param error_threshold Maximum allowable error
    * @param max_iterations Maximum allowable iterations
    *
    * @param fun Function to iterate over; Takes a weight vector to update
    *   and an int (iteration number), returns a tuple (num_errors,
    *   total_adjustment) where `num_errors` is the number of errors made on
    *   the training data and `total_adjustment` is the total sum of the
    *   scaling factors used to update the weights when a mistake is made. */
  def iterate_averaged(weights: VectorAggregate,
        averaged: Boolean, error_threshold: Double, max_iterations: Int)(
      fun: (VectorAggregate, Int) => (Int, Int, Double)) = {
    if (!averaged) {
      val num_iterations =
        iterate(error_threshold, max_iterations){ iter =>
          fun(weights, iter)
        }
      (weights, num_iterations)
    } else {
      val avg_weights = new_zero_weights(weights.length)
      val num_iterations =
        iterate(error_threshold, max_iterations){ iter =>
          val retval = fun(weights, iter)
          avg_weights += weights
          retval
        }
      avg_weights *= 1.0 / num_iterations
      (avg_weights, num_iterations)
    }
  }

  /** Compute the weights used to initialize a linear classifier.
    *
    * @return Tuple of weights and number of iterations required
    *   to compute them. */
  def get_weights(data: Iterable[(DI, Int)]): (VectorAggregate, Int)

  /** Create a linear classifier given weights and a function to format
    * feature indices into user-readable strings. */
  def create_classifier(weights: VectorAggregate,
      format_feature_fn: Int => String): LinearClassifier

  /** Train a linear classifier given a set of labeled instances. */
  def apply(data: Iterable[(DI, Int)]) = {
    val (weights, _) = get_weights(data)
    val format_feature_fn = data.head._1.feature_vector.format_feature _
    val cfier = create_classifier(weights, format_feature_fn)
    if (debug("weights"))
      cfier.debug_print_weights()
    cfier
  }
}

/**
 * Mix-in for training a classifier that supports the possibility of multiple
 * correct labels for a given training instance.
 *
 * @tparam DI Type of data instance used in training the classifier.
 */
trait MultiCorrectLabelClassifierTrainer[DI <: DataInstance]
    extends ClassifierTrainer[DI] {
  /** Return set of "yes" labels associated with an instance.  Currently only
    * one yes label per instance, but this could be changed by redoing this
    * function. */
  def yes_labels(inst: DI, label: Int) =
    (0 until 0) ++ (label to label)

  /** Return set of "no" labels associated with an instance -- complement of
    * the set of "yes" labels. */
  def no_labels(inst: DI, label: Int) =
    (0 until label) ++ ((label + 1) until number_of_labels(inst))
}
 
/**
 * Class for training a linear classifier with a single weight vector for
 * all labels.  This is used both for binary and multi-label classifiers.
 * In the binary case, having a single set of weights is natural, since
 * we use a single predicted score to specify both which of the two
 * labels to choose and how much confidence we have in the label.
 *
 * A single-weight multi-label classifier, i.e. there is a single combined
 * weight array for all labels, where the array is indexed both by feature
 * and label and can supply an arbitrary mapping down to the actual vector
 * of vector of values used to store the weights.  This allows for all sorts
 * of schemes that tie some weights to others, e.g. tying two features
 * together across all labels, or two labels together across a large subset
 * of features.  This, naturally, subsumes the multi-weight variant as a
 * special case.
 *
 * In the multi-label case, having a single weight vector is less obvious
 * because there are multiple scores predicted, generally one per potential
 * label.  In a linear classifier, each score is predicted using a
 * dot product of a weight vector with the instance's feature vector,
 * meaning there needs to be a separately trained set of weights for each
 * label.  The single weight vector actually includes all weights for all
 * labels concatenated inside it in some fashion; see the description in
 * `SingleWeightMultiLabelLinearClassifier`.
 */
trait SingleWeightLinearClassifierTrainer[DI <: DataInstance]
    extends LinearClassifierTrainer[DI]
    with VariableDepthClassifierLike[DI] {
  val vector_factory: SimpleVectorFactory
  lazy val factory = new SingleVectorAggregateFactory(vector_factory)

  def create_classifier(weights: VectorAggregate,
      format_feature_fn: Int => String) = {
    new VariableDepthLinearClassifier(weights) {
      override def format_feature(index: Int) = format_feature_fn(index)
    }
  }
}

/**
 * Class for training a multi-weight linear classifier., i.e. there is a
 * different set of weights for each label.  Note that this can be subsumed
 * as a special case of the single-weight multi-label classifier because the
 * feature vectors are indexed by label.  To do this, expand the weight vector
 * to include subsections for each possible label, and expand the feature
 * vectors so that the feature vector corresponding to a given label has
 * the original feature vector occupying the subsection corresponding to
 * that label and 0 values in all other subsections.
 */
trait MultiWeightLinearClassifierTrainer[DI <: DataInstance]
    extends LinearClassifierTrainer[DI]
    with FixedDepthClassifierLike[DI] {
  val vector_factory: SimpleVectorFactory
  lazy val factory = new MultiVectorAggregateFactory(vector_factory, num_labels)

  def create_classifier(weights: VectorAggregate,
      format_feature_fn: Int => String) = {
    new FixedDepthLinearClassifier(weights, num_labels) {
      override def format_feature(index: Int) = format_feature_fn(index)
    }
  }
}

/**
 * Class for training a binary linear classifier given a set of training
 * instances and associated labels.
 */
trait BinaryLinearClassifierTrainer[DI <: DataInstance]
    extends LinearClassifierTrainer[DI]
    with BinaryClassifierLike[DI] {
  val vector_factory: SimpleVectorFactory
  lazy val factory = new SingleVectorAggregateFactory(vector_factory)

  def create_classifier(weights: VectorAggregate,
      format_feature_fn: Int => String) = {
    new BinaryLinearClassifier(weights.asInstanceOf[SingleVectorAggregate]) {
      override def format_feature(index: Int) = format_feature_fn(index)
    }
  }
}

/**
 * Class for training a single-weight multi-label linear classifier.
 */
trait SingleWeightMultiLabelLinearClassifierTrainer[DI <: DataInstance]
  extends SingleWeightLinearClassifierTrainer[DI] {
}

/**
 * Class for training a multi-weight multi-label linear classifier.
 */
trait MultiWeightMultiLabelLinearClassifierTrainer[DI <: DataInstance]
  extends MultiWeightLinearClassifierTrainer[DI] {
}
