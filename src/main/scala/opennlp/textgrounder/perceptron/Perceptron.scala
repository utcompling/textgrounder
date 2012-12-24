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
package perceptron

/**
 * A perceptron for binary classification.
 *
 * @author Ben Wing
 */

import scala.util.control.Breaks._
import collection.mutable
import io.Source

import util.print._
import util.memoizer._
import util.metering._
import gridlocate.GridLocateDriver.Debug._

/**
 * A vector of real-valued features.  In general, features are indexed
 * both by a non-negative integer and by a class label (i.e. a label for
 * the class that is associated with a particular instance by a classifier).
 * Commonly, the class label is ignored when looking up a feature's value.
 * Some implementations might want to evaluate the features on-the-fly
 * rather than store an actual vector of values.
 */
trait FeatureVector {
  /** Return the length of the feature vector.  This is the number of weights
    * that need to be created -- not necessarily the actual number of items
    * stored in the vector (which will be different especially in the case
    * of sparse vectors). */
  def length: Int

  /** Return the number of items stored in the vector.  This will be different
    * from the length in the case of sparse vectors. */
  def stored_entries: Int

  /** Return the value at index `i`, for class `label`. */
  def apply(i: Int, label: Int): Double

  /** Return the squared magnitude of the feature vector for class `label`,
    * i.e. dot product of feature vector with itself */
  def squared_magnitude(label: Int): Double

  /** Return the squared magnitude of the difference between the values of
    * this feature vector for the two labels `label1` and `label2`. */
  def diff_squared_magnitude(label1: Int, label2: Int): Double

  /** Return the dot product of the given weight vector with the feature
    * vector for class `label`. */
  def dot_product(weights: WeightVector, label: Int): Double

  /** Update a weight vector by adding a scaled version of the feature vector,
    * with class `label`. */
  def update_weights(weights: WeightVector, scale: Double, label: Int)
}

/**
 * A feature vector that ignores the class label.
 */
trait SimpleFeatureVector extends FeatureVector {
  /** Return the value at index `i`. */
  def apply(i: Int): Double

  def apply(i: Int, label: Int) = apply(i)
}

/**
 * A feature vector in which the features are stored densely, i.e. as
 * an array of values.
 */
trait DenseFeatureVector extends FeatureVector {
  def stored_entries = length

  def dot_product(weights: WeightVector, label: Int) =
    (for (i <- 0 until length) yield apply(i, label)*weights(i)).sum

  def squared_magnitude(label: Int) =
    (for (i <- 0 until length; va = apply(i, label)) yield va*va).sum

  def diff_squared_magnitude(label1: Int, label2: Int) =
    (for (i <- 0 until length; va = apply(i, label1) - apply(i, label2))
       yield va*va).sum

  def update_weights(weights: WeightVector, scale: Double, label: Int) {
    (0 until length).foreach { i => weights(i) += scale*apply(i, label) }
  }
}

/**
 * A vector of real-valued features, stored explicitly.  The values passed in
 * are used exactly as the values of the feature; no additional term is
 * inserted to handle a "bias" or "intercept" weight.
 */
class RawArrayFeatureVector(
  values: WeightVector
) extends DenseFeatureVector with SimpleFeatureVector {
  /** Add two feature vectors. */
  def +(other: SimpleFeatureVector) = {
    val len = length
    val res = new WeightVector(len)
    for (i <- 0 until len)
      res(i) = this(i) + other(i)
    new RawArrayFeatureVector(res)
  }
  
  /** Subtract two feature vectors. */
  def -(other: SimpleFeatureVector) = {
    val len = length
    val res = new WeightVector(len)
    for (i <- 0 until len)
      res(i) = this(i) - other(i)
    new RawArrayFeatureVector(res)
  }

  /** Scale a feature vector. */
  def *(scalar: Double) = {
    val len = length
    val res = new WeightVector(len)
    for (i <- 0 until len)
      res(i) = this(i)*scalar
    new RawArrayFeatureVector(res)
  }

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
class ArrayFeatureVector(
  values: WeightVector
) extends DenseFeatureVector with SimpleFeatureVector {
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

/**
 * A factory object for creating sparse feature vectors for classification.
 */
class SparseFeatureVectorFactory[T](
  display_feature: T => String
) {
  // Use Trove for fast, efficient hash tables.
  val hashfact = new TroveHashTableFactory
  // Alternatively, just use the normal Scala hash tables.
  // val hashfact = new ScalaHashTableFactory

  // Set the minimum index to 1 so we can use 0 for the intercept term
  val feature_mapper = new ToIntMemoizer[T](hashfact, minimum_index = 1)

  trait SparseFeatureVectorLike {
    def length = {
      // +1 because of the intercept term
      feature_mapper.number_of_entries + 1
    }

    def diff_squared_magnitude(label1: Int, label2: Int) = 0.0

    def compute_toString(prefix: String,
        feature_values: Iterable[(Int, Double)]) =
      "%s(%s)" format (prefix,
        feature_values.filter { case (index, value) => index > 0 }.
          toSeq.sorted.map {
            case (index, value) =>
              "%s(%s)=%.2f" format (
                display_feature(feature_mapper.unmemoize(index)),
                index, value
              )
          }.mkString(",")
      )
  }

  /**
   * A feature vector in which the features are stored sparsely, i.e. only
   * the features with non-zero values are stored, using a hash table or
   * similar.  The features are indexed by integers, using the mapping
   * stored in `feature_mapper`. There will always be a feature with the
   * index 0, value 1.0, to handle the intercept term.
   */
  protected class CompressedSparseFeatureVector(
    keys: Array[Int], values: Array[Double]
  ) extends SimpleFeatureVector with SparseFeatureVectorLike {
    assert(keys.length == values.length)

    def stored_entries = keys.length

    def apply(index: Int) = {
      val keyind = java.util.Arrays.binarySearch(keys, index)
      if (keyind < 0) 0.0 else values(keyind)
    }

    def squared_magnitude(label: Int) = {
      var i = 0
      var res = 0.0
      while (i < keys.length) {
        res += values(i) * values(i)
        i += 1
      }
      res
    }

    def dot_product(weights: WeightVector, label: Int) = {
      var i = 0
      var res = 0.0
      while (i < keys.length) {
        res += values(i) * weights(keys(i))
        i += 1
      }
      res
    }

    def update_weights(weights: WeightVector, scale: Double, label: Int) {
      var i = 0
      while (i < keys.length) {
        weights(keys(i)) += scale * values(i)
        i += 1
      }
    }

    override def toString = compute_toString(
      "CompressedSparseFeatureVector", keys zip values)
  }

  /**
   * A feature vector in which the features are stored sparsely, i.e. only
   * the features with non-zero values are stored, using a hash table or
   * similar.  The features are indexed by integers, using the mapping
   * stored in `feature_mapper`. There will always be a feature with the
   * index 0, value 1.0, to handle the intercept term.
   */
  protected abstract class SparseFeatureVector(
    feature_values: Iterable[(Int, Double)]
  ) extends SimpleFeatureVector with SparseFeatureVectorLike {
    def stored_entries = feature_values.size

    // def apply(index: Int): Double --- needs to be defined

    def squared_magnitude(label: Int) =
      feature_values.map {
        case (index, value) => value * value
      }.sum

    def dot_product(weights: WeightVector, label: Int) =
      feature_values.map {
        case (index, value) => value * weights(index)
      }.sum

    def update_weights(weights: WeightVector, scale: Double, label: Int) {
      feature_values.map {
        case (index, value) => weights(index) += scale * value
      }
    }

    def string_prefix: String

    override def toString = compute_toString(string_prefix, feature_values)
  }

  /**
   * An implementation of a sparse feature vector storing the non-zero
   * features in a `Map`.
   */
  protected class MapSparseFeatureVector(
    feature_values: collection.Map[Int, Double]
  ) extends SparseFeatureVector(feature_values) {
    def apply(index: Int) = feature_values.getOrElse(index, 0.0)

    def string_prefix = "MapSparseFeatureVector"
  }

  protected class TupleArraySparseFeatureVector(
    feature_values: mutable.Buffer[(Int, Double)]
  ) extends SparseFeatureVector(feature_values) {
    // Use an O(n) algorithm to look up a value at a given index.  Luckily,
    // this operation isn't performed very often (if at all).  We could
    // speed it up by storing the items sorted and use binary search.
    def apply(index: Int) = feature_values.find(_._1 == index) match {
      case Some((index, value)) => value
      case None => 0.0
    }

    def string_prefix = "TupleArraySparseFeatureVector"
  }

  val vector_impl = debugval("featvec") match {
    case "Compressed" | "compressed" => "Compressed"
    case "TupleArray" | "tuplearray" | "tuple-array" => "TupleArray"
    case "Map" | "map" => "Map"
    case _ => "Compressed"
  }

  errprint(vector_impl match {
    case "Compressed" => "Using compressed feature vectors"
    case "TupleArray" => "Using tuple array (semi-compressed) feature vectors"
    case "Map" => "Using map (uncompressed) feature vectors"
  })

  /**
   * Generate a feature vector.  If not at training time, we need to be
   * careful to skip features not seen during training because there won't a
   * corresponding entry in the weight vector, and the resulting feature would
   * containing a non-existent index, causing a crash during lookup (e.g.
   * during the dot-product operation).
   */
  def make_feature_vector(feature_values: Iterable[(T, Double)],
      is_training: Boolean) = {
    val memoized_features = Iterable(0 -> 1.0) ++: (// the intercept term
      if (is_training)
        feature_values.map {
          case (name, value) => (feature_mapper.memoize(name), value)
        }
       else
        for { (name, value) <- feature_values;
               index = feature_mapper.memoize_if(name);
               if index != None }
          yield (index.get, value)
      )
    vector_impl match {
      case "Compressed" => {
        val (keys, values) =
          memoized_features.toIndexedSeq.sortWith(_._1 < _._1).unzip
        new CompressedSparseFeatureVector(keys.toArray, values.toArray)
      }
      case "TupleArray" =>
        new TupleArraySparseFeatureVector(memoized_features.toBuffer)
      case "Map" =>
        new MapSparseFeatureVector(memoized_features.toMap)
    }
  }
}

/**
 * A factory object for creating sparse nominal instances for classification,
 * consisting of a nominal label and a set of nominal features.  Nominal
 * labels have no ordering or other numerical significance.
 */
class SparseNominalInstanceFactory extends
  SparseFeatureVectorFactory[String](identity) {
  val label_mapper = new ToIntMemoizer[String](hashfact, minimum_index = 0)
  def label_to_index(label: String) = label_mapper.memoize(label)
  def index_to_label(index: Int) = label_mapper.unmemoize(index)
  def number_of_labels = label_mapper.number_of_entries

  def make_labeled_instance(features: Iterable[String], label: String,
      is_training: Boolean) = {
    val featvals = features.map((_, 1.0))
    val featvec = make_feature_vector(featvals, is_training)
    // FIXME: What about labels not seen during training?
    val labelind = label_to_index(label)
    (featvec, labelind)
  }

  def get_csv_labeled_instances(source: Source, is_training: Boolean) = {
    val lines = source.getLines
    for (line <- lines) yield {
      val atts = line.split(",")
      val label = atts.last
      val features = atts.dropRight(1)
      make_labeled_instance(features, label, is_training)
    }
  }
}

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
 */
trait LinearClassifierTrainer {
  /** Create and initialize a vector of weights of length `len`.
    * By default, initialized to all 0's, but could be changed. */
  def new_weights(len: Int) = new WeightVector(len)

  /** Create and initialize a vector of weights of length `len` to all 0's. */
  def new_zero_weights(len: Int) = new WeightVector(len)

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

  /** Iterate Train a linear classifier given a set of labeled instances. */
  def iterate(error_threshold: Double, max_iterations: Int)(
      fun: Int => Double) = {
    val task = new Meter("running", "perceptron training iteration")
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

  /** Train a linear classifier given a set of labeled instances. */
  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int):
    LinearClassifier
}

/**
 * Class for training a linear classifier given a set of training instances and
 * associated labels.
 */
trait BinaryLinearClassifierTrainer extends LinearClassifierTrainer {
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

object Maxutil {
  /** Return the argument producing the maximum when the function is applied
    * to it. */
  def argmax[T](args: Iterable[T], fun: T => Double) = {
    (args zip args.map(fun)).maxBy(_._2)._1
  }

  /** Return both the argument producing the maximum and the maximum value
    * itself, when the function is applied to the arguments. */
  def argandmax[T](args: Iterable[T], fun: T => Double) = {
    (args zip args.map(fun)).maxBy(_._2)
  }

  /** Return the argument producing the minimum when the function is applied
    * to it. */
  def argmin[T](args: Iterable[T], fun: T => Double) = {
    (args zip args.map(fun)).minBy(_._2)._1
  }

  /** Return both the argument producing the minimum and the minimum value
    * itself, when the function is applied to the arguments. */
  def argandmin[T](args: Iterable[T], fun: T => Double) = {
    (args zip args.map(fun)).minBy(_._2)
  }
}

/**
 * A multi-class perceptron with only a single set of weights for all classes.
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
    Maxutil.argmax[Int](0 until number_of_labels, score_class(instance, _))

  /** Score a given instance for a single class. */
  def score_class(instance: FeatureVector, clazz: Int) =
    instance.dot_product(weights, clazz)

  /** Score a given instance, returning an array of scores, one per class. */
  def score(instance: FeatureVector) =
    (0 until number_of_labels).map(score_class(instance, _)).toArray
}

/**
 * A multi-class perceptron with a different set of weights for each class.
 * Note that the feature vector is also passed the class in when a value is
 * requested.
 */
class MultiClassLinearClassifier (
  val weights: IndexedSeq[WeightVector]
) extends LinearClassifier {
  val number_of_labels = weights.length

  assert(number_of_labels >= 2)

  /** Classify a given instance, returning the class. */
  def classify(instance: FeatureVector) =
    Maxutil.argmax[Int](0 until number_of_labels, score_class(instance, _))

  /** Score a given instance for a single class. */
  def score_class(instance: FeatureVector, clazz: Int) =
    instance.dot_product(weights(clazz), clazz)

  /** Score a given instance, returning an array of scores, one per class. */
  def score(instance: FeatureVector) =
    (0 until number_of_labels).map(score_class(instance, _)).toArray
}

/**
 * Class for training a multi-class perceptron with only a single set of
 * weights for all classes.
 */
abstract class SingleWeightMultiClassPerceptronTrainer(
  error_threshold: Double = 1e-10,
  max_iterations: Int = 1000
) extends LinearClassifierTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(data: Iterable[(FeatureVector, Int)], num_classes: Int) = {
    val len = check_sequence_lengths(data)
    assert(num_classes >= 2)
    for ((inst, label) <- data)
      assert(label >= 0 && label < num_classes)
    new_weights(len)
  }

  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int):
    SingleWeightMultiClassLinearClassifier
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
        val (r,rscore) = Maxutil.argandmin[Int](yeslabs, dotprod(_))
        val (s,sscore) = Maxutil.argandmax[Int](nolabs, dotprod(_))
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
) extends LinearClassifierTrainer {
  assert(error_threshold >= 0)
  assert(max_iterations > 0)

  /** Check that the arguments passed in are kosher, and return an array of
    * the weights to be learned. */
  def initialize(data: Iterable[(FeatureVector, Int)], num_classes: Int) = {
    val len = check_sequence_lengths(data)
    assert(num_classes >= 2)
    for ((inst, label) <- data)
      assert(label >= 0 && label < num_classes)
    IndexedSeq[WeightVector](
      (for (i <- 0 until num_classes) yield new_weights(len)) :_*)
  }

  def apply(data: Iterable[(FeatureVector, Int)], num_classes: Int):
    MultiClassLinearClassifier
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
        val (r,rscore) = Maxutil.argandmin[Int](yeslabs, dotprod(_))
        val (s,sscore) = Maxutil.argandmax[Int](nolabs, dotprod(_))
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

  def cost(correct: Int, predicted: Int): Double
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
            Maxutil.argmax[Int](all_labs, dotprod(_))
          else
            Maxutil.argmax[Int](all_labs,
              x=>(dotprod(x) - goldscore + math.sqrt(cost(label, x))))
        val loss = dotprod(predlab) - goldscore +
          math.sqrt(cost(label, predlab))
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

  def cost(correct: Int, predicted: Int): Double
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
            Maxutil.argmax[Int](all_labs, dotprod(_))
          else
            Maxutil.argmax[Int](all_labs,
              x=>(dotprod(x) - goldscore + math.sqrt(cost(label, x))))
        val loss = dotprod(predlab) - goldscore +
          math.sqrt(cost(label, predlab))
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
