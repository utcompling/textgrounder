///////////////////////////////////////////////////////////////////////////////
//  Perceptron.scala
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
package learning.perceptron

/**
 * A perceptron for statistical classification.
 *
 * @author Ben Wing
 */

import learning._
import util.math.{argmax,argmin,argandmax,argandmin}
import util.print._

/**
 * Class for training a binary perceptron given a set of training instances
 * and associated labels.  Use function application to train a new
 * perceptron, e.g. `new BinaryPerceptronTrainer()(data)`.
 *
 * The basic perceptron training algorithm, in all its variants, works as
 * follows:
 *
 * 1. We do multiple iterations, and in each iteration we loop through the
 *    training instances.
 * 2. We process the training instances one-by-one, and potentially update
 *    the weight vector each time we process a training instance. (Hence,
 *    the algorithm is "online" or sequential, as opposed to an "off-line"
 *    or batch algorithm that attempts to satisfy some globally optimal
 *    function, e.g. maximize the joint probability of seeing the entire
 *    training set.  An off-line iterative algorithm updates the weight
 *    function once per iteration in a way that attempts to improve the
 *    overall performance of the algorithm on the entire training set.)
 * 3. Each time we see a training instance, we run the prediction algorithm
 *    to see how we would do on that training instance.  In general, if
 *    we produce the right answer, we make no changes to the weights.
 *    However, if we produce the wrong answer, we change the weights in
 *    such a way that we will subsequently do better on the given training
 *    instance, generally by adding to the weight vector a simple scalar
 *    multiple (possibly negative) of the feature vector associated with the
 *    training instance in question.
 * 4. We repeat until no further change (or at least, the total change is
 *    less than some small value), or until we've done a maximum number of
 *    iterations.
 * @param error_threshold Threshold that the sum of all scale factors for
 *    all instances must be below in order for training to stop.  In
 *    practice, in order to succeed with a threshold such as 1e-10, the
 *    actual sum of scale factors must be 0.
 * @param max_iterations Maximum number of iterations.  Training stops either
 *    when the threshold constraint succeeds of the maximum number of
 *    iterations is reached.
 */
abstract class BinaryPerceptronTrainer(
  averaged: Boolean = false,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends BinaryLinearClassifierTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(data: Iterable[(FeatureVector, Int)]) = {
    val len = check_sequence_lengths(data)
    for ((inst, label) <- data)
      assert(label == 0 || label == 1)
    new_weights(len)
  }

  /** Return the scale factor used for updating the weight vector to a
    * new weight vector.
    *
    * @param inst Instance we are currently processing.
    * @param label True label of that instance.
    * @param score Predicted score on that instance.
    */
  def get_scale_factor(inst: FeatureVector, label: Int, score: Double):
    Double

  /** Train a binary perceptron given a set of labeled instances. */
  def apply(data: Iterable[(FeatureVector, Int)]) = {
    val debug = false
    val weights = initialize(data)
    val avg_weights = new_zero_weights(weights.length)
    def print_weights() {
      errprint("Weights: length=%s,max=%s,min=%s",
        weights.length, weights.max, weights.min)
      // errprint("Weights: [%s]", weights.mkString(","))
    }
    if (debug)
      print_weights()
    val num_iterations =
      iterate(error_threshold, max_iterations) { iter =>
        var total_error = 0.0
        if (debug)
          errprint("Iteration %s", iter)
        for ((inst, label) <- data) {
          if (debug)
            errprint("Instance %s, label %s", inst, label)
          val score = inst.dot_product(weights, 1)
          if (debug)
            errprint("Score %s", score)
          val scale = get_scale_factor(inst, label, score)
          if (debug)
            errprint("Scale %s", scale)
          inst.update_weights(weights, scale, 1)
          if (debug)
            print_weights()
          total_error += math.abs(scale)
        }
        if (averaged)
          (0 until weights.length).foreach(i => avg_weights(i) += weights(i))
        total_error
      }
    if (averaged) {
      (0 until weights.length).foreach(i => avg_weights(i) /= num_iterations)
      new BinaryLinearClassifier(avg_weights)
    } else new BinaryLinearClassifier(weights)
  }
}

/** Train a binary perceptron using the basic algorithm.  See the above
  * description of the general perceptron training algorithm.  In this case,
  * when we process an instance, if our prediction is wrong, we either
  * push the weight up (if the correct prediction is positive) or down (if the
  * correct prediction is negative), according to `alpha` times the feature
  * vector of the instance we just evaluated on.
  */
class BasicBinaryPerceptronTrainer(
  alpha: Double,
  averaged: Boolean = false,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends BinaryPerceptronTrainer(averaged, error_threshold, max_iterations) {
  def get_scale_factor(inst: FeatureVector, label: Int, score: Double) = {
    val pred = if (score > 0) 1 else -1
    // Map from 0/1 to -1/1
    val symmetric_label = label*2 - 1
    alpha*(symmetric_label - pred)
  }
}

trait PassiveAggressivePerceptronTrainer {
  val _variant: Int
  val _aggressiveness_param: Double

  def compute_update_factor(loss: Double, sqmag: Double) = {
    assert(_variant >= 0 && _variant <= 2)
    assert(_aggressiveness_param > 0)
    if (_variant == 0)
      loss / sqmag
    else if (_variant == 1)
      _aggressiveness_param min (loss / sqmag)
    else
      loss / (sqmag + 1.0/(2.0*_aggressiveness_param))
  }

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

/** Train a binary perceptron using the basic algorithm.  See the above
  * description of the general perceptron training algorithm.  When processing
  * a training instance, the algorithm is "passive" in the sense that it makes
  * no changes if the prediction is correct (as in all perceptron training
  * algorithms), and "aggressive" when a prediction is wrong in the sense that
  * it changes the weight as much as necessary (but no more) to satisfy a
  * given constraint.  In this case, the idea is to change the weight as
  * little as possible while ensuring that the prediction on the instance is
  * not only correct but has a score that exceeds the minimally required score
  * for correctness by at least as much as a given "margin".  Hence, we
  * essentially * try to progess as much as possible in each step (the
  * constraint satisfaction) while also trying to preserve as much information
  * as possible that was learned previously (the minimal constraint
  * satisfaction).
  *
  * @param variant Variant 0 directly implements the algorithm just
  *  described.  The other variants are designed for training sets that may
  *  not be linearly separable, and as a result are less aggressive.
  *  Variant 1 simply limits the total change to be no more than a given
  *  factor, while variant 2 scales the total change down relatively.  In
  *  both cases, an "aggressiveness factor" needs to be given.
  * @param aggressiveness_param As just described above.  Higher values
  *  cause more aggressive changes to the weight vector during training.
  */
class PassiveAggressiveBinaryPerceptronTrainer(
  variant: Int,
  aggressiveness_param: Double = 20.0,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends BinaryPerceptronTrainer(false, error_threshold, max_iterations)
    with PassiveAggressivePerceptronTrainer {
  val _variant = variant; val _aggressiveness_param = aggressiveness_param
  def get_scale_factor(inst: FeatureVector, label: Int, score: Double) = {
    // Map from 0/1 to -1/1
    val symmetric_label = label*2 - 1
    val loss = 0.0 max (1.0 - symmetric_label*score)
    val sqmag = inst.squared_magnitude(1)
    compute_update_factor(loss, sqmag)*symmetric_label
  }
}

/**
 * Class for training a multi-class perceptron with only a single set of
 * weights for all classes.
 */
abstract class SingleWeightMultiClassPerceptronTrainer(
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends SingleWeightMultiClassLinearClassifierTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)
}

/**
 * Class for training a passive-aggressive multi-class perceptron with only a
 * single set of weights for all classes.
 */
class PassiveAggressiveSingleWeightMultiClassPerceptronTrainer(
  variant: Int,
  aggressiveness_param: Double = 20.0,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends SingleWeightMultiClassPerceptronTrainer(
  error_threshold, max_iterations
) with PassiveAggressivePerceptronTrainer {
  val _variant = variant; val _aggressiveness_param = aggressiveness_param

  /**
   * Actually train a passive-aggressive single-weight multi-class
   * perceptron.  Note that, although we're passed in a single correct label
   * per instance, the code below is written so that it can handle a set of
   * correct labels; you'd just have to change `yes_labels` and `no_labels`
   * and pass the appropriate set of correct labels in.
   */
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int) = {
    val weights = initialize(data, num_classes)
    iterate(error_threshold, max_iterations) { iter =>
      var total_error = 0.0
      for ((inst, label) <- data) {
        def dotprod(x: Int) = inst.dot_product(weights, x)
        val yeslabs = yes_labels(label, num_classes)
        val nolabs = no_labels(label, num_classes)
        val (r,rscore) = argandmin[Int](yeslabs, dotprod(_))
        val (s,sscore) = argandmax[Int](nolabs, dotprod(_))
        val margin = rscore - sscore
        val loss = 0.0 max (1.0 - margin)
        val sqmagdiff = inst.diff_squared_magnitude(r, s)
        val scale = compute_update_factor(loss, sqmagdiff)
        inst.update_weights(weights, scale, r)
        inst.update_weights(weights, -scale, s)
        total_error += math.abs(scale)
      }
      total_error
    }
    new SingleWeightMultiClassLinearClassifier(weights, num_classes)
  }
}

/**
 * Class for training a multi-class perceptron with separate weights for each
 * class.
 */
abstract class MultiClassPerceptronTrainer(
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends MultiClassLinearClassifierTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)
}

/**
 * Class for training a passive-aggressive multi-class perceptron with only a
 * single set of weights for all classes.
 */
class PassiveAggressiveMultiClassPerceptronTrainer(
  variant: Int,
  aggressiveness_param: Double = 20.0,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends MultiClassPerceptronTrainer(
  error_threshold, max_iterations
) with PassiveAggressivePerceptronTrainer {
  val _variant = variant; val _aggressiveness_param = aggressiveness_param

  /**
   * Actually train a passive-aggressive multi-weight multi-class
   * perceptron.  Note that, although we're passed in a single correct label
   * per instance, the code below is written so that it can handle a set of
   * correct labels; you'd just have to change `yes_labels` and `no_labels`
   * and pass the appropriate set of correct labels in.
   */
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int) = {
    val weights = initialize(data, num_classes)
    iterate(error_threshold, max_iterations) { iter =>
      var total_error = 0.0
      for ((inst, label) <- data) {
        def dotprod(x: Int) = inst.dot_product(weights(x), x)
        val yeslabs = yes_labels(label, num_classes)
        val nolabs = no_labels(label, num_classes)
        val (r,rscore) = argandmin[Int](yeslabs, dotprod(_))
        val (s,sscore) = argandmax[Int](nolabs, dotprod(_))
        val margin = rscore - sscore
        val loss = 0.0 max (1.0 - margin)
        val rmag = inst.squared_magnitude(r)
        val smag = inst.squared_magnitude(s)
        val sqmagdiff = rmag + smag
        val scale = compute_update_factor(loss, sqmagdiff)
        inst.update_weights(weights(r), scale, r)
        inst.update_weights(weights(s), -scale, s)
        total_error += math.abs(scale)
      }
      total_error
    }
    new MultiClassLinearClassifier(weights)
  }
}

/**
 * Class for training a cost-sensitive multi-class perceptron with only a
 * single set of weights for all classes.
 */
abstract class CostSensitiveSingleWeightMultiClassPerceptronTrainer(
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends SingleWeightMultiClassPerceptronTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)

  def cost(inst: FeatureVector, correct: Int, predicted: Int): Double
}

/**
 * Class for training a passive-aggressive cost-sensitive multi-class
 * perceptron with only a single set of weights for all classes.
 */
abstract class PassiveAggressiveCostSensitiveSingleWeightMultiClassPerceptronTrainer(
  prediction_based: Boolean,
  variant: Int,
  aggressiveness_param: Double = 20.0,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends CostSensitiveSingleWeightMultiClassPerceptronTrainer(
  error_threshold, max_iterations
) with PassiveAggressivePerceptronTrainer {
  val _variant = variant; val _aggressiveness_param = aggressiveness_param

  /**
   * Actually train a passive-aggressive single-weight multi-class
   * perceptron.  Note that, although we're passed in a single correct label
   * per instance, the code below is written so that it can handle a set of
   * correct labels; you'd just have to change `yes_labels` and `no_labels`
   * and pass the appropriate set of correct labels in.
   */
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int) = {
    val weights = initialize(data, num_classes)
    var iter = 0
    val all_labs = 0 until num_classes
    iterate(error_threshold, max_iterations) { iter =>
      var total_error = 0.0
      for ((inst, label) <- data) {
        def dotprod(x: Int) = inst.dot_product(weights, x)
        val goldscore = dotprod(label)
        val predlab =
          if (prediction_based)
            argmax[Int](all_labs, dotprod(_))
          else
            argmax[Int](all_labs,
              x=>(dotprod(x) - goldscore + math.sqrt(cost(inst, label, x))))
        val loss = dotprod(predlab) - goldscore +
          math.sqrt(cost(inst, label, predlab))
        val sqmagdiff = inst.diff_squared_magnitude(label, predlab)
        val scale = compute_update_factor(loss, sqmagdiff)
        inst.update_weights(weights, scale, label)
        inst.update_weights(weights, -scale, predlab)
        total_error += math.abs(scale)
      }
      total_error
    }
    new SingleWeightMultiClassLinearClassifier(weights, num_classes)
  }
}

/**
 * Class for training a cost-sensitive multi-class perceptron with a separate
 * set of weights per class.
 */
abstract class CostSensitiveMultiClassPerceptronTrainer(
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends MultiClassPerceptronTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)

  def cost(inst: FeatureVector, correct: Int, predicted: Int): Double
}

/**
 * Class for training a passive-aggressive cost-sensitive multi-class
 * perceptron with a separate set of weights per class.
 */
abstract class PassiveAggressiveCostSensitiveMultiClassPerceptronTrainer(
  prediction_based: Boolean,
  variant: Int,
  aggressiveness_param: Double = 20.0,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends CostSensitiveMultiClassPerceptronTrainer(
  error_threshold, max_iterations
) with PassiveAggressivePerceptronTrainer {
  val _variant = variant; val _aggressiveness_param = aggressiveness_param

  /**
   * Actually train a passive-aggressive single-weight multi-class
   * perceptron.  Note that, although we're passed in a single correct label
   * per instance, the code below is written so that it can handle a set of
   * correct labels; you'd just have to change `yes_labels` and `no_labels`
   * and pass the appropriate set of correct labels in.
   */
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int) = {
    val weights = initialize(data, num_classes)
    val all_labs = 0 until num_classes
    iterate(error_threshold, max_iterations) { iter =>
      var total_error = 0.0
      for ((inst, label) <- data) {
        def dotprod(x: Int) = inst.dot_product(weights(x), x)
        val goldscore = dotprod(label)
        val predlab =
          if (prediction_based)
            argmax[Int](all_labs, dotprod(_))
          else
            argmax[Int](all_labs,
              x=>(dotprod(x) - goldscore + math.sqrt(cost(inst, label, x))))
        val loss = dotprod(predlab) - goldscore +
          math.sqrt(cost(inst, label, predlab))
        val rmag = inst.squared_magnitude(label)
        val smag = inst.squared_magnitude(predlab)
        val sqmagdiff = rmag + smag
        val scale = compute_update_factor(loss, sqmagdiff)
        inst.update_weights(weights(label), scale, label)
        inst.update_weights(weights(predlab), -scale, predlab)
        total_error += math.abs(scale)
      }
      total_error
    }
    new MultiClassLinearClassifier(weights)
  }
}
