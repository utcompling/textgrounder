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
 * A basic linear classifier.
 */
trait LinearClassifier {
  /** Return number of labels. */
  def number_of_labels: Int

  /** Classify a given instance, returning the class (a label from 0 to
    * `number_of_labels`-1). */
  def classify(instance: FeatureVector): Int

  /** Score a given instance.  Return a sequence of predicted scores, of
    * the same length as the number of labels present.  There is one score
    * per label, and the maximum score corresponds to the single predicted
    * label if such a prediction is desired. */
  def score(instance: FeatureVector): IndexedSeq[Double]
}

/**
 * A binary linear classifier, created from an array of weights.  Normally
 * created automatically by one of the trainer classes.
 */
class BinaryLinearClassifier (
  val weights: WeightVector
) extends LinearClassifier {
  val number_of_labels = 2

  /** Classify a given instance, returning the class, either 0 or 1. */
  def classify(instance: FeatureVector) = {
    val sc = binary_score(instance)
    if (sc > 0) 1 else 0
  }

  /** Score a given instance, returning a single real number.  If the score
    * is &gt; 0, 1 is predicted, else 0. */
  def binary_score(instance: FeatureVector) = instance.dot_product(weights, 1)

  def score(instance: FeatureVector) =
      IndexedSeq(0, binary_score(instance))
}

/**
 * Class for training a linear classifier given a set of training instances and
 * associated labels.
 *
 * @tparam WVType Type of weight-vector object.  Will hold a single weight vector
 *   for single-weight variants and multiple weight vectors for multi-weight
 *   variants.
 */
trait LinearClassifierTrainer[WVType] {
  type WV = WVType

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(data: Iterable[(FeatureVector, Int)], num_classes: Int) = {
    val len = check_sequence_lengths(data)
    assert(num_classes >= 2)
    for ((inst, label) <- data)
      assert(label >= 0 && label < num_classes)
    new_weights(len, num_classes)
  }

  /** Create and initialize a vector of weights of length `len`.
    * By default, initialized to all 0's, but could be changed. */
  def new_weights(len: Int, num_classes: Int) =
    new_zero_weights(len, num_classes)

  /** Create and initialize a vector of weights of length `len` to all 0's. */
  def new_zero_weights(len: Int, num_classes: Int): WV

  def weight_length(wv: WV): Int

  def weight_for_label(weights: WV, label: Int): WeightVector

  def add_weight_to_weight(accum: WV, addto: WV)

  def divide_weight_by_value(accum: WV, value: Double)

  /** Check that all instances have the same length, and return it. */
  def check_sequence_lengths(data: Iterable[(FeatureVector, Int)]) = {
    // Written this way because the length might change as we iterate
    // the first time through the data (this will be the case if we are
    // using SparseFeatureVector). The call to `max` iterates through the
    // whole data first before checking the length again. (Previously we
    // compared the first against the rest, which ran into problems.)
    val len = data.map(_._1.length).max
    for ((inst, label) <- data) {
      assert(inst.length == len)
    }
    len
  }

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
  def iterate_averaged(num_classes: Int, weights: WV, averaged: Boolean,
        error_threshold: Double, max_iterations: Int)(
      fun: (WV, Int) => Double) = {
    if (!averaged) {
      val num_iterations =
        iterate(error_threshold, max_iterations){ iter =>
          fun(weights, iter)
        }
      (weights, num_iterations)
    } else {
      val avg_weights = new_zero_weights(weight_length(weights), num_classes)
      val num_iterations =
        iterate(error_threshold, max_iterations){ iter =>
          val total_error = fun(weights, iter)
          add_weight_to_weight(avg_weights, weights)
          total_error
        }
      divide_weight_by_value(avg_weights, num_iterations)
      (avg_weights, num_iterations)
    }
  }

  /** Train a linear classifier given a set of labeled instances. */
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int):
    LinearClassifier
}

 
/**
 * Class for training a linear classifier with a single weight vector for
 * all lablels.
 */
trait SingleWeightLinearClassifierTrainer extends
    LinearClassifierTrainer[WeightVector] {
  /** Create and initialize a vector of weights of length `len` to all 0's. */
  def new_zero_weights(len: Int, num_classes: Int) = new WeightVector(len)

  final def weight_length(wv: WV) = wv.length

  final def weight_for_label(weights: WV, label: Int) = weights

  def add_weight_to_weight(accum: WV, addto: WV) {
    (0 until addto.length).foreach(i => accum(i) += addto(i))
  }

  def divide_weight_by_value(wv: WV, value: Double) {
    (0 until wv.length).foreach(i => wv(i) /= value)
  }
}

/**
 * Class for training a multi-weight linear classifier.
 */
trait MultiWeightLinearClassifierTrainer extends
    LinearClassifierTrainer[IndexedSeq[WeightVector]] {

  /** Create and initialize a vector of weights of length `len` to all 0's. */
  def new_zero_weights(len: Int, num_classes: Int) =
    IndexedSeq[WeightVector](
      (for (i <- 0 until num_classes) yield new WeightVector(len)) :_*)

  final def weight_length(wv: WV) = wv(0).length

  final def weight_for_label(weights: WV, label: Int) = weights(label)

  def add_weight_to_weight(accum: WV, addto: WV) {
    for (j <- 0 until accum.length) {
      val awv = accum(j)
      val bwv = addto(j)
      assert(awv.length == bwv.length)
      (0 until awv.length).foreach(i => awv(i) += bwv(i))
    }
  }

  def divide_weight_by_value(wv: WV, value: Double) {
    for (j <- 0 until wv.length) {
      val awv = wv(j)
      (0 until awv.length).foreach(i => awv(i) /= value)
    }
  }
}

/**
 * Class for training a linear classifier given a set of training instances and
 * associated labels.
 */
trait BinaryLinearClassifierTrainer extends SingleWeightLinearClassifierTrainer {
  /** Train a linear classifier given a set of labeled instances. */
  def apply(data: Iterable[(FeatureVector, Int)]): 
    BinaryLinearClassifier

  /** Train a linear classifier given a set of labeled instances. */
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int) = {
    assert(num_classes == 2)
    apply(data)
  }
}

/**
 * A multi-class classifier with only a single set of weights for all classes.
 * Note that the feature vector is passed the class in when a value is
 * requested; it is assumed that class-specific features are handled
 * automatically through this mechanism.
 */
class SingleWeightMultiClassLinearClassifier (
  val weights: WeightVector,
  val number_of_labels: Int
) extends LinearClassifier {

  assert(number_of_labels >= 2)

  /** Classify a given instance, returning the class. */
  def classify(instance: FeatureVector) =
    argmax[Int](0 until number_of_labels, score_class(instance, _))

  /** Score a given instance for a single class. */
  def score_class(instance: FeatureVector, clazz: Int) =
    instance.dot_product(weights, clazz)

  /** Score a given instance, returning an array of scores, one per class. */
  def score(instance: FeatureVector) =
    (0 until number_of_labels).map(score_class(instance, _)).toArray
}

/**
 * A multi-class classifier with a different set of weights for each class.
 * Note that the feature vector is also passed the class in when a value is
 * requested.
 */
class MultiWeightMultiClassLinearClassifier (
  val weights: IndexedSeq[WeightVector]
) extends LinearClassifier {
  val number_of_labels = weights.length

  assert(number_of_labels >= 2)

  /** Classify a given instance, returning the class. */
  def classify(instance: FeatureVector) =
    argmax[Int](0 until number_of_labels, score_class(instance, _))

  /** Score a given instance for a single class. */
  def score_class(instance: FeatureVector, clazz: Int) =
    instance.dot_product(weights(clazz), clazz)

  /** Score a given instance, returning an array of scores, one per class. */
  def score(instance: FeatureVector) =
    (0 until number_of_labels).map(score_class(instance, _)).toArray
}

/**
 * Mix-in for training a multi-class linear classifier.
 */
trait MultiClassLinearClassifierTrainer {
  /** Return set of "yes" labels associated with an instance.  Currently only
    * one yes label per instance, but this could be changed by redoing this
    * function. */
  def yes_labels(label: Int, num_classes: Int) =
    (0 until 0) ++ (label to label)

  /** Return set of "no" labels associated with an instance -- complement of
    * the set of "yes" labels. */
  def no_labels(label: Int, num_classes: Int) =
    (0 until label) ++ (label until num_classes)
}

/**
 * Class for training a multi-class linear classifier with only a single set of
 * weights for all classes.
 */
trait SingleWeightMultiClassLinearClassifierTrainer
  extends SingleWeightLinearClassifierTrainer
  with MultiClassLinearClassifierTrainer {
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int):
    SingleWeightMultiClassLinearClassifier
}

/**
 * Class for training a multi-class linear classifier with separate weights for each
 * class.
 */
trait MultiWeightMultiClassLinearClassifierTrainer
  extends MultiWeightLinearClassifierTrainer
  with MultiClassLinearClassifierTrainer {
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int):
    MultiWeightMultiClassLinearClassifier
}
