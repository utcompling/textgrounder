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
import learning._

import util.debug._

/**
 * A perceptron for statistical classification.
 *
 * @author Ben Wing
 *
 * The binary perceptron training algorithm works as follows:
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
 *    we produce the right answer -- or at least, the right answer with a
 *    sufficient amount of confidence -- we make no changes to the weights.
 *    However, if we produce the wrong answer, we change the weights in
 *    such a way that we will subsequently do better on the given training
 *    instance, generally by adding to the weight vector a simple scalar
 *    multiple (possibly negative) of the feature vector associated with the
 *    training instance in question.
 * 4. We keep iterating until some stopping criterion -- generally, either
 *    the total change is less than some threshold value, or a maximum
 *    number of iterations is reached.
 *
 * In the binary perceptron algorithm, each instance is described by a
 * feature vector (a fixed-size vector of real-valued features), and there
 * is a single vector of weights to be learned, with one weight per feature.
 * We name each feature by an integer index starting at 0, for easy indexing
 * into a feature vector or weight vector. Given a learned weight vector,
 * we classify a given instance using a confidence score computed simply by
 * a dot product of the weight vector and the instance's feature vector.
 * In the binary case, the sign of the score indicates which of the two
 * possible labels to predict, and the magnitude indicates the relative
 * confidence.  Note that, in general the relative confidence cannot be
 * related to any absolute measure (e.g. probability); all that can be said
 * is that a score with higher magnitude indicates greater confidence than
 * one with a lower magnitude, provided that both scores came from the
 * same perceptron.
 *
 * In the multi-label perceptron algorithm, there are multiple possible
 * labels that can be predicted.  In this situation, we compute a separate
 * score for each possible label -- again, using a dot product of a learned
 * weight vector and a feature vector derived from the instance in question --
 * and choose the label with the highest score.  In this situation, the
 * sign and magnitude of the scores themselves have no significance.  The only
 * thing that matters is the difference between two scores, which indicates
 * a relative confidence that one label is better than the other.
 * 
 * In the multi-label case, there are two variants for handling the weights:
 * Either we use a single weight vector for all labels or maintain a separate
 * weight vector for each label.  The former case is most appropriate when
 * an instance has a separate feature vector for each label being predicted
 * (or at least, the feature values differ significantly from label to label).
 * The latter is required in the more common case where an instance is
 * described by a single feature vector that is independent of the label,
 * although it can also be used when there are separate per-label feature
 * vectors associated with an instance.  In the discussion below, we term
 * these variants "single-weight" and "multi-weight", respectively. (Crammer
 * et al.'s 2006 paper on passive-aggressive perceptrons uses
 * "single-prototype" and "multi-prototype", respectively.)
 *
 * Note that the multi-weight variant can be subsumed by the single-weight
 * variant as follows: Given N features and K labels, create a single
 * weight vector of length NK.  Then create K separate feature vectors of
 * length NK, with the kth original feature vector occupying positions in
 * the range [Nk, N(k+1)) in the kth new vector and all other positions
 * having 0's.  A similar arrangement could be used to create various sorts
 * of arrangements with partly tied weights.
 *
 * In the multi-label case, every time we need to update weights, we update
 * the weights associated both with the correct label and the incorrectly
 * predicted label.  As in the binary case, we add a scalar multiple of the
 * appropriate feature vector to each of the two weight vectors (or to the
 * same weight vector if there is only one).  The two weight vectors in
 * general use scalar multiples with the same magnitudes but opposite signs,
 * so that they get pushed in opposite directions (biasing the weights
 * towards the correct label and away from the incorrect one).
 *
 * The basic algorithm is very simple: Whenever a label is correctly
 * predicted, no weights are changed, and in other cases, a fixed,
 * pre-specified scalar multiple is used (the "learning rate", or equivalently
 * the amount of progress made in each step).  The passive-aggressive
 * algorithm is different in various ways:
 *
 * -- Rather than simply attempting to ensure that training instances are
 *    correctly labeled (i.e. in a graphical sense, it attempts to locate
 *    some arbitrary hyperplane separating the positive from negative
 *    instances, or in the multi-label case, some arbitrary set of hyperplanes
 *    separating the instances for a given label from all others), it
 *    attempts to ensure not only that all instances are correctly labeled
 *    but that the following constraint is met: the difference between the
 *    score for the correct label and all other labels exceeds some minimum
 *    margin.  This is essentially equivalent to seeking the maximum-margin
 *    separating hyperplane, much like what an SVM (support vector machine)
 *    does.
 * -- Hence, weights are updated both if training instances are incorrectly
 *    labeled and if they are labeled correctly but with insufficient
 *    confidence (i.e. margin).
 * -- Furthermore, the scalar multiple used to update the weights is not
 *    fixed but (at least in the simplest variant) is set as large as
 *    necessary to ensure that the minimum-margin constraint is satisfied.
 * -- Hence, the algorithm is "passive" in making no change if the margin
 *    constraint is already satisfied for a given instance, and otherwise is
 *    "aggressive" in changing the weights as much as necessary to satisfy
 *    the constraint.
 * -- The above description is only strictly correct in describing the
 *    most basic variant of the passive-aggressive algorithm ("variant 0").
 *    The other two variants use an additional parameter to limit the
 *    "aggressiveness" of the algorithm (i.e. the size of the scalar multiple
 *    used to update weights), either by setting a hard maximum on the
 *    scalar update factor or by scaling the predicted value downward
 *    non-linearly, such that higher values are scaled downward more
 *    aggressively than lower values.
 *
 * An additional variant found below, the "cost-sensitive" variant, allows
 * for the situation where some incorrect labels are worse than others.
 * This is controlled by specifying a "cost" for predicting a given
 * incorrect label for a given instance. (Geolocation with a grid of cells
 * placed over the Earth is a good example. Some incorrect cells are near
 * the correct one, while some are quite far away. The efficacy of geolocation
 * is judged not by the number of correctly-predicted cells but by the
 * average or median error distance. Hence, for best results this error
 * distance needs to be incorporated into the learning algorithm.)
 *
 * To use a perceptron for ranking, proceed as follows:
 *
 * 1. Our job is, given a query and a set of candidates, rank the candidates
 *    according to how well they fit the query.  Ranking using a perceptron
 *    or other linear classifier is usually (using a "pointwise ranker"),
 *    performed simply by scoring each one standardly (using a dot product),
 *    and ranking according to descending sort order of the score.
 * 2. For the purposes of ranking, in place of the normal feature vector
 *    describing a candidate, we have we have a "ranking feature vector"
 *    describing the combination of query and candidate, which includes
 *    (in additional to some sort of invididual features describing only the
 *    query or only the candidate) some "combined features" describing both
 *    query and candidate, e.g. indicating how well they match in certain
 *    respects (e.g. presence of certain words in both).
 * 3. Our training data consists of instances, each of which contains a
 *    query, a set of candidates, a "ranking feature vector" corresponding
 *    to each candidate (describing the query-candidate pair), and usually
 *    the true ranking over the candidates.  In some cases we are only told
 *    which candidate is the correct one (or in some cases, there may be
 *    more than one correct candidate given).  The ranking may well be
 *    included as one of the features in the ranking feature vector for a
 *    candidate. In general, the number of candidates may vary from instance
 *    to instance.
 * 4. To rank using a perceptron (and probably also any other scoring linear
 *    classifier), treat it as a single-weight multi-label classification
 *    problem, where the correct candidate is associated with the "correct"
 *    label, and the labels of all other candidates are given "incorrect"
 *    labels.
 * 6. Note also that the cost-insensitive algorithms are capable of supporting
 *    multiple correct candidates with some work done to the scaffolding,
 *    and for the cost-sensitive algorithms, choose as "correct" the one
 *    with the least error cost, and if there are two that have the same
 *    cost, pick one as correct and give the other one zero cost. (Remember,
 *    the cost returned by the cost function is the additional cost of
 *    choosing an "incorrect" label *relative* to the "correct" label.)
 */

import learning._
import util.math.{argmax,argmin,argandmax,argandmin}
import util.print._

trait PerceptronTrainer {
  val averaged: Boolean
  val error_threshold: Double
  val max_iterations: Int
  assert(error_threshold >= 0)
  assert(max_iterations > 0)
}

/**
 * Class for training a binary perceptron given a set of training instances
 * and associated labels.  Use function application to train a new
 * perceptron.
 *
 * @param averaged Whether to average the weights predicted at each successive
 *    step or simply to use the weights determined at the last step.
 *    Averaging is helpful especially in cases where a separating hyperplane
 *    cannot be found and as a result the weights oscillate around some
 *    central values that are more optimal than any of the values at a given
 *    step. (This is generally not useful in conjunction with the
 *    passive-aggressive algorithm, which tends to steadily push the weights
 *    closer to their optimal value.)
 * @param error_threshold Threshold that the sum of all scale factors for
 *    all instances must be below in order for training to stop.  In
 *    practice, in order to succeed with a threshold such as 1e-10, the
 *    actual sum of scale factors must be 0.
 * @param max_iterations Maximum number of iterations.  Training stops either
 *    when the threshold constraint succeeds of the maximum number of
 *    iterations is reached.
 */
trait BinaryPerceptronTrainer
    extends BinaryLinearClassifierTrainer[FeatureVector]
    with PerceptronTrainer {
  /** Return the scale factor used for updating the weight vector to a
    * new weight vector.  This will be 0 if no updating should be
    * done. (In the basic perceptron algorithm, this happens whenever
    * the predicted label is correct.  In the passive-aggressive
    * algorithm, this happens when the predicted label is correct and
    * the confidence exceeds a given threshold.)
    *
    * @param inst Instance we are currently processing.
    * @param symmetric_label True label of that instance, in a symmetric
    *   form (-1 or 1)
    * @param margin "Margin" between correct and incorrect label.  In this
    *   case this is the score, scaled by -1 if the correct label is -1,
    *   so positive means correct.
    */
  def get_scale_factor(inst: FeatureVector, symmetric_label: Int,
      margin: Double): Double

  def debug_get_weights(data: Iterable[(FeatureVector, Int)]
      ): (VectorAggregate, Int) = {
    val weight_aggr = initialize(data)
    val weights = weight_aggr(0)
    def print_weights() {
       errprint("Weights: length=%s,max=%s,min=%s",
         weights.length, weights.max, weights.min)
      // errprint("Weights: [%s]", weights.mkString(","))
    }
    print_weights()
    iterate_averaged(weight_aggr, averaged, error_threshold, max_iterations) {
        (weights, iter) =>
      var total_adjustment = 0.0
      var num_errors = 0
      var num_adjustments = 0
      errprint("Iteration %s", iter)
      for ((fv, label) <- data) {
        errprint("Instance %s, label %s", fv, label)
        val score = fv.dot_product(weights(0), 1)
        errprint("Score %s", score)
        // Map from 0/1 to -1/1
        val symmetric_label = label*2 - 1
        val margin = symmetric_label*score
        errprint("Margin %s", margin)
        if (margin < 0)
          num_errors += 1
        val scale = get_scale_factor(fv, symmetric_label, margin)
        errprint("Scale %s", scale)
        if (scale != 0) {
          fv.update_weights(weights(0), scale, 1)
          print_weights()
          total_adjustment += math.abs(scale)
          num_adjustments += 1
        }
      }
      (num_errors, num_adjustments, total_adjustment)
    }
  }

  def get_weights(data: Iterable[(FeatureVector, Int)]
      ): (VectorAggregate, Int) = {
    if (debug("perceptron"))
      return debug_get_weights(data)
    val weight_aggr = initialize(data)
    val weights = weight_aggr(0)
    iterate_averaged(weight_aggr, averaged, error_threshold, max_iterations) {
        (weights, iter) =>
      var total_adjustment = 0.0
      var num_errors = 0
      var num_adjustments = 0
      for ((fv, label) <- data) {
        val score = fv.dot_product(weights(0), 1)
        // Map from 0/1 to -1/1
        val symmetric_label = label*2 - 1
        val margin = symmetric_label*score
        if (margin < 0)
          num_errors += 1
        val scale = get_scale_factor(fv, symmetric_label, margin)
        if (scale != 0) {
          fv.update_weights(weights(0), scale, 1)
          total_adjustment += math.abs(scale)
          num_adjustments += 1
        }
      }
      (num_errors, num_adjustments, total_adjustment)
    }
  }
}

/**
 * Train a binary perceptron using the basic algorithm.  See the above
 * description of the general perceptron training algorithm.  In this case,
 * when we process an instance, if our prediction is wrong, we either
 * push the weight up (if the correct prediction is positive) or down (if the
 * correct prediction is negative), according to `alpha` times the feature
 * vector of the instance we just evaluated on.
 */
class BasicBinaryPerceptronTrainer(
  val vector_factory: SimpleVectorFactory,
  val alpha: Double,
  val averaged: Boolean = false,
  val error_threshold: Double = 1e-10,
  val max_iterations: Int = 1000
) extends BinaryPerceptronTrainer {
  def get_scale_factor(inst: FeatureVector, symmetric_label: Int,
      margin: Double) = {
    if (margin > 0)
      0.0
    else
      alpha
  }
}

/**
 * Mix-in for training a perceptron using one of the variants of the
 * passive-aggressive algorithm.
 */
trait PassiveAggressivePerceptronTrainer {
  val variant: Int
  val aggressiveness_param: Double

  assert(variant >= 0 && variant <= 2)
  assert(aggressiveness_param > 0)

  /** Compute the scale factor used to update weights, given the "loss"
    * from choosing a particular label for an instance in place of the
    * correct label, as well as the square magnitude of the difference
    * between the feature vectors associated with the two labels.
    * Both values cannot be negative.
    *
    * In the cost-sensitive variant, the loss is simply the margin
    * between the scores of the best-scoring label and the correct label,
    * plus the user-supplied cost of choosing the best-scoring label in
    * place of the correct one.  If the best-scoring label *is* the
    * correct one, the loss will be 0.
    *
    * In the cost-insensitive variant, however, the loss is computed
    * based on the margin between the best-scoring *incorrect* label and
    * the correct one.  In this case, the margin will be either positive
    * or negative (depending on whether the correct label is the
    * best-scoring one).  The loss is related to the margin using a
    * "hinge-loss" function such that the loss is 0 only if the correct
    * label is the best-scoring one *and* exceeds the best-scoring
    * incorrect label by a given quantity.  This turns out to make the
    * passive-aggressive algorithm search for the maximum-margin weights,
    * similar to an SVM.
    */
  def compute_update_factor(loss: Double, sqmag: Double) = {
    /* The situation where the square magnitude is 0 would seem to lead
       to division-by-zero errors and imply an infinite update factor.
       In fact, however, the vector whose scaled version is used to
       update the weights is exactly the same vector used to compute
       the square magnitude, and hence when this magnitude is 0, the
       update vector is also a zero vector and no change to the weights
       should occur.
       
       This occurs in the single-weight case when different labels
       have the same feature vector associated with them.  In the
       multiple-weight case, all labels of a given instance normally
       have the same feature vector, but the update vector is calculated
       differently such that it can be a zero vector only when the feature
       vector associated with an instance is itself a zero vector. (This
       different calculation comes from the fact that conceptually the
       multiple-weight case is mapped to a single-weight case where the
       (normally) unique feature vector for an instance is mapped to
       multiple much larger label-specific vectors that have the original
       vector occurring as a subvector starting at a label-specific
       position, and 0's everywhere else.)
      */
    if (sqmag == 0)
      0
    else if (variant == 0)
      loss / sqmag
    else if (variant == 1)
      aggressiveness_param min (loss / sqmag)
    else
      loss / (sqmag + 1.0/(2.0*aggressiveness_param))
  }
}

/*
 * Train a binary perceptron using the passive-aggressive algorithm.  See the
 * package documentation for a description of this algorithm.
 *
 * @param variant Variant 0 directly implements the passive-aggressive
 *   algorithm with no restrictions on the magnitude of weight updates at
 *   each step. The other variants are designed for training sets that may
 *   not be linearly separable, and as a result are less aggressive.
 *   Variant 1 simply sets a hard limit on the update factor, while variant
 *   2 scales the update factor down non-linearly, with larger factors
 *   reduced more.  In both cases, an "aggressiveness parameter" needs to
 *   be given.
 * @param aggressiveness_param As just described above.  Higher values
 *   allow more aggressive changes to the weight vector during training.
 */
class PassiveAggressiveBinaryPerceptronTrainer(
  val vector_factory: SimpleVectorFactory,
  val variant: Int,
  val aggressiveness_param: Double = 20.0,
  val error_threshold: Double = 1e-10,
  val max_iterations: Int = 1000
) extends { val averaged = false }
    with BinaryPerceptronTrainer
    with PassiveAggressivePerceptronTrainer {
  def get_scale_factor(inst: FeatureVector, symmetric_label: Int,
      margin: Double) = {
    val loss = 0.0 max (1.0 - margin)
    val sqmag = inst.squared_magnitude(1)
    compute_update_factor(loss, sqmag)*symmetric_label
  }
}

/**
 * Class for training a multi-label perceptron.
 */
trait MultiLabelPerceptronTrainer[DI <: DataInstance]
    extends LinearClassifierTrainer[DI]
       with PerceptronTrainer {
}

/**
 * Class for training a multi-label perceptron with only a single set of
 * weights for all labels.
 */
trait SingleWeightMultiLabelPerceptronTrainer[DI <: DataInstance]
    extends SingleWeightMultiLabelLinearClassifierTrainer[DI]
       with MultiLabelPerceptronTrainer[DI] {
}

/**
 * Class for training a multi-label perceptron with separate weights for each
 * label.
 */
trait MultiWeightMultiLabelPerceptronTrainer[DI <: DataInstance]
    extends MultiWeightMultiLabelLinearClassifierTrainer[DI]
       with MultiLabelPerceptronTrainer[DI] {
}

/**
 * Class for training a multi-label perceptron that is cost-insensitive (i.e.
 * the cost of choosing an incorrect label is the same for all labels).
 *
 * Note that the algorithm used below allows for the possibility of multiple
 * correct labels for a given instance. To support this, redefine
 * `yes_labels` and `no_labels`. Note that these functions are passed in the
 * value of the single "correct" label included with the training data, but
 * this value isn't otherwise used; if these functions don't use this value,
 * any arbitrary valid (i.e. within-range) value can be substituted.
 * (FIXME: To avoid this, add a `get_correct_label` function to
 * `ClassifierTrainer` and include the correct label as part of the
 * data instance passed in.)
 */
trait NoCostMultiLabelPerceptronTrainer[DI <: DataInstance]
    extends MultiLabelPerceptronTrainer[DI]
       with MultiCorrectLabelClassifierTrainer[DI] {
  /** Return the scale factor used for updating the weight vector to a
    * new weight vector.  This will be 0 if no updating should be
    * done. (In the basic perceptron algorithm, this happens whenever
    * the predicted label is correct.  In the passive-aggressive
    * algorithm, this happens when the predicted label is correct and
    * the confidence exceeds a given threshold.)
    *
    * @param inst Instance we are currently processing.
    * @param min_yes_label Label with minimum score among all
    *   correct labels.
    * @param max_yes_label Label with maximum score among all
    *   incorrect labels.
    * @param margin Signed margin between minimum yes-label score and
    *   maximum no-label score.
    */
  def get_scale_factor(inst: FeatureVector, min_yes_label: Int,
      max_no_label: Int, margin: Double): Double

  def debug_get_weights(data: Iterable[(DI, Int)]
      ): (VectorAggregate, Int) = {
    val weights = initialize(data)
    def print_weights() {
       errprint("Weights: length=%s,max=%s,min=%s",
         weights.length, weights.max, weights.min)
      // errprint("Weights: [%s]", weights.mkString(","))
    }
    iterate_averaged(weights, averaged, error_threshold, max_iterations) {
        (weights, iter) =>
      var total_adjustment = 0.0
      var num_errors = 0
      var num_adjustments = 0
      errprint("Iteration %s", iter)
      for ((inst, correct) <- data) {
        val fv = inst.feature_vector
        errprint("Instance %s, correct label %s", inst, correct)
        def dotprod(x: Int) = {
          val res = fv.dot_product(weights(x), x)
          errprint("Dot product inst . weights(%s) = %s", x, res)
          res
        }
        val yeslabs = yes_labels(inst, correct)
        errprint("Yes labels for correct label %s = %s", correct, yeslabs)
        val nolabs = no_labels(inst, correct)
        errprint("No labels for correct label %s = %s", correct, nolabs)
        val (r,rscore) = argandmin(yeslabs) { dotprod(_) }
        errprint("r,rscore = %s,%s", r, rscore)
        val (s,sscore) = argandmax(nolabs) { dotprod(_) }
        errprint("s,sscore = %s,%s", s, sscore)
        val margin = rscore - sscore
        errprint("margin = %s", margin)
        if (margin < 0)
          num_errors += 1
        val scale = get_scale_factor(fv, r, s, margin)
        errprint("scale = %s", scale)
        if (scale != 0) {
          fv.update_weights(weights(r), scale, r)
          fv.update_weights(weights(s), -scale, s)
          print_weights()
          total_adjustment += math.abs(scale)
          num_adjustments += 1
        }
      }
      (num_errors, num_adjustments, total_adjustment)
    }
  }

  def get_weights(data: Iterable[(DI, Int)]
      ): (VectorAggregate, Int) = {
    if (debug("perceptron"))
      return debug_get_weights(data)
    val weights = initialize(data)
    iterate_averaged(weights, averaged, error_threshold, max_iterations) {
        (weights, iter) =>
      var total_adjustment = 0.0
      var num_errors = 0
      var num_adjustments = 0
      for ((inst, correct) <- data) {
        val fv = inst.feature_vector
        def dotprod(x: Int) =
          fv.dot_product(weights(x), x)
        val yeslabs = yes_labels(inst, correct)
        val nolabs = no_labels(inst, correct)
        val (r,rscore) = argandmin(yeslabs) { dotprod(_) }
        val (s,sscore) = argandmax(nolabs) { dotprod(_) }
        val margin = rscore - sscore
        if (margin < 0)
          num_errors += 1
        val scale = get_scale_factor(fv, r, s, margin)
        if (scale != 0) {
          fv.update_weights(weights(r), scale, r)
          fv.update_weights(weights(s), -scale, s)
          total_adjustment += math.abs(scale)
          num_adjustments += 1
        }
      }
      errprint("Pct error: %s/%s = %.2f", num_errors, data.size,
        num_errors.toDouble / data.size * 100)
      (num_errors, num_adjustments, total_adjustment)
    }
  }
}

/**
 * Class for training a cost-insensitive multi-label perceptron that is
 * single-weight -- i.e. there is a single combined weight array for all
 * labels, where the array is indexed both by feature and label and
 * can supply an arbitrary mapping down to the actual set of values
 * used to store the weights.  This allows for all sorts of schemes that
 * tie some weights to others, e.g. tying two features together across
 * all labels, or two labels together across a large subset of features.
 * This, naturally, subsumes the multi-weight variant as a special case.
 */
trait NoCostSingleWeightMultiLabelPerceptronTrainer[DI <: DataInstance]
    extends SingleWeightMultiLabelPerceptronTrainer[DI]
    with NoCostMultiLabelPerceptronTrainer[DI] {
}

/**
 * Class for training a cost-insensitive multi-weight multi-label perceptron.
 */
trait NoCostMultiWeightMultiLabelPerceptronTrainer[DI <: DataInstance]
    extends MultiWeightMultiLabelPerceptronTrainer[DI]
    with NoCostMultiLabelPerceptronTrainer[DI] {
}

/**
 * Mix-in for training a multi-label perceptron without cost-sensitivity,
 * using the basic algorithm.  In this case, if we predict a correct label,
 * we don't change the weights; otherwise, we simply use a specified scale
 * factor.
 */
trait BasicNoCostMultiLabelPerceptronTrainer {
  val alpha: Double

  def get_scale_factor(inst: FeatureVector, min_yes_label: Int,
      max_no_label: Int, margin: Double) = {
    if (margin > 0)
      0.0
    else
      alpha
  }
}

/**
 * Train a single-weight multi-label perceptron without cost-sensitivity,
 * using the basic algorithm.  In this case, if we predict a correct label,
 * we don't change the weights; otherwise, we simply use a specified scale
 * factor.
 */
class BasicSingleWeightMultiLabelPerceptronTrainer[DI <: DataInstance](
  val vector_factory: SimpleVectorFactory,
  val alpha: Double,
  val averaged: Boolean = false,
  val error_threshold: Double = 1e-10,
  val max_iterations: Int = 1000
) extends NoCostSingleWeightMultiLabelPerceptronTrainer[DI]
     with BasicNoCostMultiLabelPerceptronTrainer {
}

/**
 * Train a multi-weight multi-label perceptron without cost-sensitivity,
 * using the basic algorithm.  In this case, if we predict a correct label,
 * we don't change the weights; otherwise, we simply use a specified scale
 * factor.
 */
class BasicMultiWeightMultiLabelPerceptronTrainer[DI <: DataInstance](
  val vector_factory: SimpleVectorFactory,
  val num_labels: Int,
  val alpha: Double,
  val averaged: Boolean = false,
  val error_threshold: Double = 1e-10,
  val max_iterations: Int = 1000
) extends NoCostMultiWeightMultiLabelPerceptronTrainer[DI]
     with BasicNoCostMultiLabelPerceptronTrainer {
}

/**
 * Mix-in for training a passive-aggressive single-weight multi-label
 * perceptron.
 */
trait PassiveAggressiveSingleWeightMultiLabelPerceptronTrainer
    extends PassiveAggressivePerceptronTrainer {
  def scale_factor_from_loss(inst: FeatureVector, correct: Int, predicted: Int,
      loss: Double) = {
    val sqmagdiff = inst.diff_squared_magnitude(correct, predicted)
    compute_update_factor(loss, sqmagdiff)
  }
}

/**
 * Mix-in for training a passive-aggressive multi-weight multi-label
 * perceptron.
 */
trait PassiveAggressiveMultiWeightMultiLabelPerceptronTrainer
    extends PassiveAggressivePerceptronTrainer {
  def scale_factor_from_loss(inst: FeatureVector, correct: Int, predicted: Int,
      loss: Double) = {
    val rmag = inst.squared_magnitude(correct)
    val smag = inst.squared_magnitude(predicted)
    val sqmagdiff = rmag + smag
    compute_update_factor(loss, sqmagdiff)
  }
}

/**
 * Mix-in for training a passive-aggressive cost-insensitive multi-label
 * perceptron.
 */
trait PassiveAggressiveNoCostMultiLabelPerceptronTrainer {
  def scale_factor_from_loss(inst: FeatureVector, correct: Int, predicted: Int,
      loss: Double): Double

  def get_scale_factor(inst: FeatureVector, min_yes_label: Int,
      max_no_label: Int, margin: Double) = {
    val loss = 0.0 max (1.0 - margin)
    scale_factor_from_loss(inst, min_yes_label, max_no_label, loss)
  }
}

/**
 * Train a single-weight multi-label perceptron without cost-sensitivity,
 * using a passive-aggressive algorithm.
 */
class PassiveAggressiveNoCostSingleWeightMultiLabelPerceptronTrainer[
  DI <: DataInstance
](
  val vector_factory: SimpleVectorFactory,
  val variant: Int,
  val aggressiveness_param: Double = 20.0,
  val error_threshold: Double = 1e-10,
  val max_iterations: Int = 1000
) extends { val averaged = false }
     with NoCostSingleWeightMultiLabelPerceptronTrainer[DI]
     with PassiveAggressiveNoCostMultiLabelPerceptronTrainer
     with PassiveAggressiveSingleWeightMultiLabelPerceptronTrainer {
}

/**
 * Train a multi-weight multi-label perceptron without cost-sensitivity,
 * using a passive-aggressive algorithm.
 */
class PassiveAggressiveNoCostMultiWeightMultiLabelPerceptronTrainer[
  DI <: DataInstance
](
  val vector_factory: SimpleVectorFactory,
  val num_labels: Int,
  val variant: Int,
  val aggressiveness_param: Double = 20.0,
  val error_threshold: Double = 1e-10,
  val max_iterations: Int = 1000
) extends { val averaged = false }
     with NoCostMultiWeightMultiLabelPerceptronTrainer[DI]
     with PassiveAggressiveNoCostMultiLabelPerceptronTrainer
     with PassiveAggressiveMultiWeightMultiLabelPerceptronTrainer {
}

/**
 * Class for training a cost-sensitive multi-label perceptron.
 */
trait CostSensitiveMultiLabelPerceptronTrainer[DI <: DataInstance]
    extends MultiLabelPerceptronTrainer[DI] {
  /** If true, use the prediction-based cost-sensitive algorithm.
    * Otherwise, use the max-loss algorithm.
    */
  val prediction_based: Boolean

  /** Return the scale factor used for updating the weight vector to a
    * new weight vector.  This will be 0 if no updating should be
    * done.
    *
    * @param inst Instance we are currently processing.
    * @param correct Correct label.
    * @param predicted Predicted label.
    * @param loss Loss from choosing predicted in place of correct label
    *   (includes cost from the `cost` function).
    */
  def get_scale_factor(inst: FeatureVector, correct: Int, predicted: Int,
    loss: Double): Double

  /** Return the cost of predicting an incorrect label for a given instance.
    *
    * @param inst Instance we are currently processing.
    * @param correct Correct label.
    * @param predicted Predicted label.
    */
  def cost(inst: DI, correct: Int, predicted: Int): Double

  def debug_get_weights(data: Iterable[(DI, Int)]
      ): (VectorAggregate, Int) = {
    val weights = initialize(data)
    def print_weights() {
       errprint("Weights: length=%s,max=%s,min=%s",
         weights.length, weights.max, weights.min)
      // errprint("Weights: [%s]", weights.mkString(","))
    }
    def get_cost(inst: DI, correct: Int, predicted: Int) = {
      val costval = cost(inst, correct, predicted)
      errprint("Cost (correct = %s, predicted = %s): %s",
        correct, predicted, costval)
      costval
    }

    iterate_averaged(weights, averaged, error_threshold, max_iterations) {
        (weights, iter) =>
      var total_adjustment = 0.0
      var num_errors = 0
      var num_adjustments = 0
      errprint("Iteration %s", iter)
      for ((inst, correct) <- data) {
        val fv = inst.feature_vector
        errprint("Instance %s, correct label %s", inst, correct)
        val all_labs = 0 until number_of_labels(inst)
        errprint("All labels: %s", all_labs)
        def dotprod(x: Int) = {
          val res = fv.dot_product(weights(x), x)
          errprint("Dot product inst . weights(%s) = %s", x, res)
          res
        }
        val goldscore = dotprod(correct)
        errprint("Gold score: %s", goldscore)
        val predicted =
          if (prediction_based)
            argmax(all_labs) { dotprod(_) }
          else
            argmax(all_labs) {
              x=>(dotprod(x) - goldscore +
                math.sqrt(get_cost(inst, correct, x)))
            }
        errprint("Predicted label (%s): %s",
          if (prediction_based) "prediction-based" else "max-loss", predicted)
        if (predicted != correct) {
          num_errors += 1
          val loss = dotprod(predicted) - goldscore +
            math.sqrt(get_cost(inst, correct, predicted))
          errprint("Loss: %s", loss)
          val scale = get_scale_factor(fv, correct, predicted, loss)
          errprint("Scale: %s", scale)
          if (scale != 0) {
            fv.update_weights(weights(correct), scale, correct)
            fv.update_weights(weights(predicted), -scale, predicted)
            print_weights()
            total_adjustment += math.abs(scale)
            num_adjustments += 1
          }
        }
      }
      (num_errors, num_adjustments, total_adjustment)
    }
  }

  def get_weights(data: Iterable[(DI, Int)]): (VectorAggregate, Int) = {
    if (debug("perceptron"))
      return debug_get_weights(data)
    val weights = initialize(data)
    iterate_averaged(weights, averaged, error_threshold,
         max_iterations) { (weights, iter) =>
      var total_adjustment = 0.0
      var num_errors = 0
      var num_adjustments = 0
      for ((inst, correct) <- data) {
        val fv = inst.feature_vector
        val all_labs = 0 until number_of_labels(inst)
        def dotprod(x: Int) = fv.dot_product(weights(x), x)
        val goldscore = dotprod(correct)
        val predicted =
          if (prediction_based)
            argmax(all_labs) { dotprod(_) }
          else
            argmax(all_labs) {
              x=>(dotprod(x) - goldscore + math.sqrt(cost(inst, correct, x)))
            }
        if (predicted != correct) {
          num_errors += 1
          val loss = dotprod(predicted) - goldscore +
            math.sqrt(cost(inst, correct, predicted))
          val scale = get_scale_factor(fv, correct, predicted, loss)
          if (scale != 0) {
            fv.update_weights(weights(correct), scale, correct)
            fv.update_weights(weights(predicted), -scale, predicted)
            total_adjustment += math.abs(scale)
            num_adjustments += 1
          }
        }
      }
      (num_errors, num_adjustments, total_adjustment)
    }
  }
}

/**
 * Class for training a multi-label perceptron with only a single set of
 * weights for all labels, with different costs for choosing different incorrect
 * labels.
 */
trait CostSensitiveSingleWeightMultiLabelPerceptronTrainer[DI <: DataInstance]
    extends SingleWeightMultiLabelPerceptronTrainer[DI]
    with CostSensitiveMultiLabelPerceptronTrainer[DI] {
}

/**
 * Class for training a multi-label perceptron with separate weights for each
 * label, with different costs for choosing different incorrect labels.
 */
trait CostSensitiveMultiWeightMultiLabelPerceptronTrainer[DI <: DataInstance]
    extends MultiWeightMultiLabelPerceptronTrainer[DI]
    with CostSensitiveMultiLabelPerceptronTrainer[DI] {
}

/**
 * Mix-in for training a passive-aggressive cost-sensitive multi-label
 * perceptron.
 */
trait PassiveAggressiveCostSensitiveMultiLabelPerceptronTrainer {
  def scale_factor_from_loss(inst: FeatureVector, correct: Int, predicted: Int,
      loss: Double): Double

  def get_scale_factor(inst: FeatureVector, correct: Int, predicted: Int,
      loss: Double) =
    scale_factor_from_loss(inst, correct, predicted, loss)
}


/**
 * Train a single-weight cost-sensitive multi-label perceptron,
 * using a passive-aggressive algorithm.
 */
abstract class PassiveAggressiveCostSensitiveSingleWeightMultiLabelPerceptronTrainer[
  DI <: DataInstance
](
  val vector_factory: SimpleVectorFactory,
  val prediction_based: Boolean,
  val variant: Int,
  val aggressiveness_param: Double = 20.0,
  val error_threshold: Double = 1e-10,
  val max_iterations: Int = 1000
) extends { val averaged = false }
     with CostSensitiveSingleWeightMultiLabelPerceptronTrainer[DI]
     with PassiveAggressiveCostSensitiveMultiLabelPerceptronTrainer
     with PassiveAggressiveSingleWeightMultiLabelPerceptronTrainer {
}

/**
 * Train a multi-weight cost-sensitive multi-label perceptron,
 * using a passive-aggressive algorithm.
 */
abstract class PassiveAggressiveCostSensitiveMultiWeightMultiLabelPerceptronTrainer[
  DI <: DataInstance
](
  val vector_factory: SimpleVectorFactory,
  val prediction_based: Boolean,
  val num_labels: Int,
  val variant: Int,
  val aggressiveness_param: Double = 20.0,
  val error_threshold: Double = 1e-10,
  val max_iterations: Int = 1000
) extends { val averaged = false }
     with CostSensitiveMultiWeightMultiLabelPerceptronTrainer[DI]
     with PassiveAggressiveCostSensitiveMultiLabelPerceptronTrainer
     with PassiveAggressiveMultiWeightMultiLabelPerceptronTrainer {
}
