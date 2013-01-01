///////////////////////////////////////////////////////////////////////////////
//  Classifier.scala
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
trait Classifier {
  /** Return number of labels associated with a given instance. (In
    * fixed-depth classifiers, this value is the same for all instances.) */
  def number_of_labels(inst: FeatureVector): Int

  /** Classify a given instance, returning the label (from 0 to
    * `number_of_labels`-1). */
  def classify(inst: FeatureVector): Int
}

trait ScoringClassifier extends Classifier {
  /** Score a given instance for a single label. */
  def score_label(inst: FeatureVector, label: Int): Double

  def classify(inst: FeatureVector) =
    argmax[Int](0 until number_of_labels(inst), score_label(inst, _))

  /** Score a given instance.  Return a sequence of predicted scores, of
    * the same length as the number of labels present.  There is one score
    * per label, and the maximum score corresponds to the single predicted
    * label if such a prediction is desired. */
  def score(inst: FeatureVector): IndexedSeq[Double] =
    (0 until number_of_labels(inst)).map(score_label(inst, _)).toIndexedSeq
}

abstract class LinearClassifier(
  val weights: VectorAggregate
) extends ScoringClassifier {
  def score_label(inst: FeatureVector, label: Int) =
    inst.dot_product(weights(label), label)
}

/** Mix-in for a fixed-depth linear classifier (or trainer thereof). */
trait FixedDepthLinearClassifierLike {
  val num_labels: Int
  def number_of_labels(inst: FeatureVector) = num_labels
}

/** Mix-in for a variable-depth linear classifier (or trainer thereof). */
trait VariableDepthLinearClassifierLike {
  def number_of_labels(inst: FeatureVector) = inst.depth
}

trait BinaryLinearClassifierLike extends FixedDepthLinearClassifierLike {
  val num_labels = 2
}

class FixedDepthLinearClassifier(weights: VectorAggregate, val num_labels: Int)
    extends LinearClassifier(weights) with FixedDepthLinearClassifierLike {
}

class VariableDepthLinearClassifier(weights: VectorAggregate)
    extends LinearClassifier(weights) with VariableDepthLinearClassifierLike {
}

/**
 * A binary linear classifier, created from an array of weights.  Normally
 * created automatically by one of the trainer classes.
 */
class BinaryLinearClassifier (
  weights: SingleVectorAggregate
) extends LinearClassifier(weights) with BinaryLinearClassifierLike {

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
 * Class for training a linear classifier given a set of training instances and
 * associated labels.
 *
 * Note that some trainers support the possibility of multiple correct labels
 * for a given training instance.  The functions `yes_labels` and
 * `no_labels` are stand-ins for this.
 */
trait LinearClassifierTrainer {
  val factory: VectorAggregateFactory

  /** Return number of labels associated with a given instance. (In
    * fixed-depth classifiers, this value is the same for all instances.) */
  def number_of_labels(inst: FeatureVector): Int

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(data: Iterable[(FeatureVector, Int)]) = {
    val len = check_sequence_lengths(data)
    val weights = new_weights(len)
    for ((inst, label) <- data)
      assert(label >= 0 && label <= (weights.max_label min inst.max_label))
    weights
  }

  /** Create and initialize a vector of weights of length `len`.
    * By default, initialized to all 0's, but could be changed. */
  def new_weights(len: Int) = new_zero_weights(len)

  /** Create and initialize a vector of weights of length `len` to all 0's. */
  def new_zero_weights(len: Int) = {
    errprint("Length of weight vector: %s", len)
    errprint("Length if we used word-dist memoization: %s",
      worddist.WordDist.memoizer.number_of_valid_indices)
    factory.empty(len)
  }

  /** Check that all instances have the same length, and return it. */
  def check_sequence_lengths(data: Iterable[(FeatureVector, Int)]) =
    FeatureVector.check_same_length(data.map(_._1))

  /** Iterate over a function to train a linear classifier. */
  def iterate(error_threshold: Double, max_iterations: Int)(
      fun: Int => Double) = {
    val task = new Meter("running", "classifier training iteration")
    task.start()
    var iter = 0
    var total_error = 0.0
    do {
      iter += 1
      total_error = fun(iter)
      errprint("Iteration %s, total_error %s", iter, total_error)
      task.item_processed()
    } while (iter < max_iterations && total_error >= error_threshold)
    task.finish()
    iter
  }

  /** Iterate over a function to train a linear classifier,
    * optionally averaging over the weight vectors at each iteration. */
  def iterate_averaged(weights: VectorAggregate,
        averaged: Boolean, error_threshold: Double, max_iterations: Int)(
      fun: (VectorAggregate, Int) => Double) = {
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
          val total_error = fun(weights, iter)
          avg_weights += weights
          total_error
        }
      avg_weights *= 1.0 / num_iterations
      (avg_weights, num_iterations)
    }
  }

  /** Train a linear classifier given a set of labeled instances. */
  def apply(data: Iterable[(FeatureVector, Int)]): LinearClassifier

  /** Return set of "yes" labels associated with an instance.  Currently only
    * one yes label per instance, but this could be changed by redoing this
    * function. */
  def yes_labels(inst: FeatureVector, label: Int) =
    (0 until 0) ++ (label to label)

  /** Return set of "no" labels associated with an instance -- complement of
    * the set of "yes" labels. */
  def no_labels(inst: FeatureVector, label: Int) =
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
trait SingleWeightLinearClassifierTrainer extends LinearClassifierTrainer
    with VariableDepthLinearClassifierLike {
  val vector_factory: SimpleVectorFactory
  lazy val factory = new SingleVectorAggregateFactory(vector_factory)
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
trait MultiWeightLinearClassifierTrainer extends LinearClassifierTrainer
    with FixedDepthLinearClassifierLike {
  val vector_factory: SimpleVectorFactory
  lazy val factory = new MultiVectorAggregateFactory(vector_factory, num_labels)
}

/**
 * Class for training a linear classifier given a set of training instances and
 * associated labels.
 */
trait BinaryLinearClassifierTrainer extends LinearClassifierTrainer
    with BinaryLinearClassifierLike {
  val vector_factory: SimpleVectorFactory
  lazy val factory = new SingleVectorAggregateFactory(vector_factory)
}

/**
 * Class for training a single-weight multi-label linear classifier.
 */
trait SingleWeightMultiLabelLinearClassifierTrainer
  extends SingleWeightLinearClassifierTrainer {
}

/**
 * Class for training a multi-weight multi-label linear classifier.
 */
trait MultiWeightMultiLabelLinearClassifierTrainer
  extends MultiWeightLinearClassifierTrainer {
}
