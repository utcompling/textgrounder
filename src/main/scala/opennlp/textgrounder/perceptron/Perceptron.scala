package opennlp.textgrounder.perceptron

/**
 * A perceptron for binary classification.
 *
 * @author Ben Wing
 */

import util.control.Breaks._

/**
 * A vector of real-valued features.  Defined as an abstract class because
 * some implementations might want to evaluate the features on-the-fly
 * rather than store an actual vector of values.
 */
abstract class FeatureVector {
  /** Return the length of the feature vector. */
  def length: Int

  /** Return the value at index `i`. */
  def apply(i: Int): Double

  /** Return the value at index `i`, for class `c`. */
  def apply(i: Int, c: Int): Double = apply(i)

  /** Add two feature vectors. */
  def +(other: FeatureVector) = {
    val len = length
    val res = new Array[Double](len)
    for (i <- 0 until len)
      res(i) = this(i) + other(i)
    new RawBasicVector(res)
  }
  
  /** Subtract two feature vectors. */
  def -(other: FeatureVector) = {
    val len = length
    val res = new Array[Double](len)
    for (i <- 0 until len)
      res(i) = this(i) - other(i)
    new RawBasicVector(res)
  }

  /** Scale a feature vector. */
  def *(scalar: Double) = {
    val len = length
    val res = new Array[Double](len)
    for (i <- 0 until len)
      res(i) = this(i)*scalar
    new RawBasicVector(res)
  }

  /** Compute the dot product of the feature vector with a set of weights,
    * for class c. */
  def dot_product(weights: Array[Double], c: Int = 1) =
    (for (i <- 0 until length) yield apply(i, c)*weights(i)).sum

  def squared_magnitude =
    (for (i <- 0 until length; va = apply(i)) yield va*va).sum
}

/**
 * A vector of real-valued features, stored explicitly.  The values passed in
 * are used exactly as the values of the feature; no additional term is
 * inserted to handle a "bias" or "intercept" weight.
 */
class RawBasicVector(
  values: Array[Double]
) extends FeatureVector {
  /** Return the length of the feature vector. */
  def length = values.length

  /** Return the value at index `i`. */
  def apply(i: Int) = values(i)

  def update(i: Int, value: Double) { values(i) = value }
}

/**
 * A vector of real-valued features, stored explicitly.  An additional value
 * set to a constant 1 is automatically stored at the end of the vector.
 */
class BasicVector(
  values: Array[Double]
) extends FeatureVector {
  /** Return the length of the feature vector; + 1 including the extra bias
    * term. */
  def length = values.length + 1

  /** Return the value at index `i`, but return 1.0 at the last index. */
  def apply(i: Int) = {
    if (i == values.length) 1.0
    else values(i)
  }

  def update(i: Int, value: Double) {
    if (i == values.length) {
      if (value != 1.0) {
        throw new IllegalArgumentException(
          "Element at the last index (index %s) unmodifiable, fixed at 1.0"
          format i)
      }
    } else { values(i) = value }
  }
}

class BinaryPerceptron (
  val weights: Array[Double]
) {
  /** Classify a given instance, returning the class, either -1 or 1. */
  def classify(instance: FeatureVector) = {
    val score = instance.dot_product(weights)
    if (score > 0) 1 else -1
  }
}

/**
 * Class for training a perceptron given a set of training instances and
 * associated labels.  The basic perceptron algorithm, in all its variants,
 * works as follows:
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
 */
abstract class PerceptronTrainer {
  /** Create and initialize a vector of weights of length `len`. */
  def new_weights(len: Int) = new Array[Double](len)

  /** Check that the number of instances matches the number of labels, and
    * that all instances have the same length. */
  def check_sequence_lengths(instances: Seq[FeatureVector], labels: Seq[Int]) {
    assert(instances.length > 0)
    assert(instances.length == labels.length)
    val len = instances(0).length
    for (inst <- instances)
      assert(inst.length == len)
  }
}

/**
 * Class for training a binary perceptron given a set of training instances
 * and associated labels.
 */
abstract class BinaryPerceptronTrainer(
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends PerceptronTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(instances: Seq[FeatureVector], labels: Seq[Int]) = {
    check_sequence_lengths(instances, labels)
    for (label <- labels)
      assert(label == 1 || label == -1)
    new_weights(instances(0).length)
  }

  /** Return the scale factor used for updating the weight vector to a
    * new weight vector. */
  def update_scale_factor(inst: FeatureVector, label: Int, score: Double):
    Double

  /** Train a binary perceptron given a set of labeled instances. */
  def apply(instances: Seq[FeatureVector], labels: Seq[Int]) = {
    val weights = initialize(instances, labels)
    val len = weights.length
    var iter = 0
    val zipped = (instances zip labels)
    while (iter < max_iterations) {
      var total_error = 0.0
      for ((inst, label) <- zipped) {
        val score = inst.dot_product(weights)
        val scale = update_scale_factor(inst, label, score)
        for (i <- 0 until len)
          weights(i) += scale*inst(i)
        total_error += math.abs(scale)
      }
      if (total_error < error_threshold)
        break
    }
    new BinaryPerceptron(weights)
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
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends BinaryPerceptronTrainer(error_threshold, max_iterations) {
  def update_scale_factor(inst: FeatureVector, label: Int, score: Double) = {
    val pred = if (score > 0) 1 else -1
    alpha*(label - pred)
  }
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
  * Variant 0 directly implements the algorithm just described.  The other
  * variants are designed for training sets that may not be linearly
  * separable, and as a result are less aggressive.  Variant 1 simply limits
  * the total change to be no more than a given factor, while variant 2 scales
  * the total change down relatively.  In both cases, an "aggressiveness
  * factor" needs to be given.
  */
class PassiveAggressiveBinaryPerceptronTrainer(
  variant: Int,
  aggressiveness_param: Double = 20.0,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends BinaryPerceptronTrainer(error_threshold, max_iterations) {
  assert(variant >= 0 && variant <= 2)
  assert(aggressiveness_param > 0)
  def update_scale_factor(inst: FeatureVector, label: Int, score: Double) = {
    val loss = 0.0 max (1 - label*score)
    val sqmag = inst.squared_magnitude
    if (variant == 0)
      loss / sqmag
    else if (variant == 1)
      aggressiveness_param min (loss / sqmag)
    else
      loss / (sqmag + 1/(2*aggressiveness_param))
  }
}

object Maxutil {
  /** Return the argument producing the maximum when the function is applied
    * to it. */
  def argmax[T](args: Seq[T], fun: T => Double) = {
    (args zip args.map(fun)).maxBy(_._2)._1
  }

  /** Return both the argument producing the maximum and the maximum value
    * itself, when the function is applied to the arguments. */
  def argandmax[T](args: Seq[T], fun: T => Double) = {
    (args zip args.map(fun)).maxBy(_._2)
  }
}

/**
 * A multi-class perceptron with only a single set of weights for all classes.
 * Note that the feature vector is passed the class in when a value is
 * requested; it is assumed that class-specific features are handled
 * automatically through this mechanism.
 */
class SingleWeightMultiClassPerceptron (
  val weights: Array[Double],
  val num_classes: Int
) {
  assert(num_classes >= 2)

  /** Classify a given instance, returning the class. */
  def classify(instance: FeatureVector) =
    Maxutil.argmax[Int](1 to num_classes, instance.dot_product(weights, _))
}

/**
 * Class for training a multi-class perceptron with only a single set of
 * weights for all classes.
 */
abstract class SingleWeightMultiClassPerceptronTrainer(
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends PerceptronTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(instances: Seq[FeatureVector], labels: Seq[Int],
      num_classes: Int) = {
    assert(num_classes >= 2)
    check_sequence_lengths(instances, labels)
    for (label <- labels)
      assert(label >= 0 && label < num_classes)
    new_weights(instances(0).length)
  }

  def apply(instances: Seq[FeatureVector], labels: Seq[Int],
    num_classes: Int): SingleWeightMultiClassPerceptron
}

/**
 * Class for training a passive-aggressive multi-class perceptron with only a
 * single set of weights for all classes.
 */
abstract class PassiveAggressiveSingleWeightMultiClassPerceptronTrainer(
  variant: Int,
  aggressiveness_param: Double = 20.0,
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends SingleWeightMultiClassPerceptronTrainer(
    error_threshold, max_iterations) {
  assert(variant >= 0 && variant <= 2)
  assert(aggressiveness_param > 0)

  def update_scale_factor(inst: FeatureVector, label: Int, score: Double) = {
    val loss = 0.0 max (1 - label*score)
    val sqmag = inst.squared_magnitude
    if (variant == 0)
      loss / sqmag
    else if (variant == 1)
      aggressiveness_param min (loss / sqmag)
    else
      loss / (sqmag + 1/(2*aggressiveness_param))
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

  /**
   * Actually train a passive-aggressive single-weight multi-class
   * perceptron.  Note that, although we're passed in a single correct label
   * per instance, the code below is written so that it can handle a set of
   * correct labels; you'd just have to change `yes_labels` and `no_labels`
   * and pass the appropriate set of correct labels in.
   */
  def apply(instances: Seq[FeatureVector], labels: Seq[Int],
      num_classes: Int) = {
    val weights = initialize(instances, labels, num_classes)
    val len = weights.length
    var iter = 0
    val zipped = (instances zip labels)
    while (iter < max_iterations) {
      var total_error = 0.0
      for ((inst, label) <- zipped) {
//        val r = label
//        val (
//
//        val margin = inst.dot_product(weights)
//        val scale = update_scale_factor(inst, label, score)
//        for (i <- 0 until len)
//          weights(i) += scale*inst(i)
//        total_error += math.abs(scale)
      }
      if (total_error < error_threshold)
        break
    }
    new SingleWeightMultiClassPerceptron(weights, num_classes)
  }
}

/**
 * A multi-class perceptron with a different set of weights for each class.
 * Note that the feature vector is also passed the class in when a value is
 * requested.
 */
class MultiClassPerceptron (
  val weights: IndexedSeq[Array[Double]]
) {
  val num_classes = weights.length
  assert (num_classes >= 2)

  /** Classify a given instance, returning the class. */
  def classify(instance: FeatureVector) =
    Maxutil.argmax[Int](1 to num_classes, x=>instance.dot_product(weights(x), x))
}

/**
 * Class for training a multi-class perceptron with separate weights for each
 * class.
 */
abstract class MultiClassPerceptronTrainer(
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends PerceptronTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(instances: Seq[FeatureVector], labels: Seq[Int],
      num_classes: Int) = {
    assert(num_classes >= 2)
    check_sequence_lengths(instances, labels)
    for (label <- labels)
      assert(label >= 0 && label < num_classes)
    val len = instances(0).length
    IndexedSeq[Array[Double]](
      (for (i <- 1 to num_classes) yield new_weights(len)) :_*)
  }

  def apply(instances: Seq[FeatureVector], labels: Seq[Int],
    num_classes: Int): MultiClassPerceptron
}

