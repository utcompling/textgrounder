///////////////////////////////////////////////////////////////////////////////
//  MLogit.scala
//
//  Copyright (C) 2013 Ben Wing, The University of Texas at Austin
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
package learning.mlogit

import learning._
import util.debug._
import util.print.{errprint,internal_error}

import org.ddahl.jvmr.RInScala

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * A conditional logit (a type of multinomial logit, which is a type of
 * GLM or generalized linear model) for solving a reranking problem.
 * This implements the equivalent of a single-weight multi-label classifier.
 *
 * @author Ben Wing
 *
 * Note that for a binary classification model, we can write logistic
 * regression as (see Wikipedia "logistic regression"):
 *
 * logit(p(y_i | X_i)) = β · x_i  for a = 1 ... N
 *
 * for the i'th training instance, with corresponding feature vector x_i,
 * weights β and binary label y_i.
 *
 * If we have K different choices, a classifier is normally written
 *
 * ln p(y_i = 1) = β_1 · x_i - ln Z
 * ln p(y_i = 2) = β_2 · x_i - ln Z
 * ...
 * ln p(y_i = K) = β_k · x_i - ln Z
 *
 * for a multinomial label y_i, with a set of weights β_k for k = 1 ... K,
 * where each choice has its own weight vector, and a normalizing function Z
 * that is required so that the probabilities all sum to 1:
 *
 * \sum_{k=1}^{K} p(y_i = k) = 1
 *
 * This is a standard multinomial logit model.
 *
 * In vector form:
 *
 * ln p([y_i = k]) = B x_i - [ln Z]
 *
 * where [y_i = k] is a vector of booleans where exactly one is true; B is
 * a matrix of of β_1,β_2,...,β_k; and [ln Z] is a vector containing the
 * same value ln Z duplicated K times.
 *
 * But in the case of a reranker, we have a set of distinct feature
 * vectors for each candidate, i.e. we have a separate set of covariates for
 * each choice. The choices are all equivalent to each other and so it makes
 * no sense to have choice-specific weights. Thus:
 *
 * ln p(y_i = 1) = β · x_{i1} - ln Z
 * ln p(y_i = 2) = β · x_{i2} - ln Z
 * ...
 * ln p(y_i = K) = β · x_{iK} - ln Z
 *
 * In this case, the distinct y_i's are simply indices noting the distinct
 * candidates, and there are choice-specific covariates x_{iK}, which can be
 * grouped into a matrix X_i. Note that for the entire set of N training
 * instances, there will be N separate matrices of covariates. These matrices
 * are often stacked together into one big matrix which each row listing
 * the covariates for a given individual (aka data instance) and choice
 * (aka candidate).
 *
 * This is a standard conditional logit model.

 * In vector form:
 *
 * ln p([y_i = k]) = X_i β - [ln Z]
 *
 * We use the 'mlogit' package in R, which is specifically designed to solve
 * models of this sort, and can do fast BFGS optimization.
 */

/**
 * Class for training a multi-label perceptron with only a single set of
 * weights for all labels.
 */
trait ConditionalLogitTrainer[DI <: DataInstance]
    extends SingleWeightMultiLabelLinearClassifierTrainer[DI] {
}

/**
 * Train a single-weight multi-label perceptron without cost-sensitivity,
 * using the basic algorithm.  In this case, if we predict a correct label,
 * we don't change the weights; otherwise, we simply use a specified scale
 * factor.
 */
class RConditionalLogitTrainer[DI <: DataInstance](
  val vector_factory: SimpleVectorFactory
) extends ConditionalLogitTrainer[DI] {

  /**
   * Filter the array to contain only the values for which the
   * corresponding entry in `specs` is true.
   */
  def filter_array[T : ClassTag](data: Array[T], specs: Array[Boolean]) = {
    assert(data.size == specs.size)
    (data zip specs).filter(_._2).map(_._1).toArray
  }

  /**
   * Filter the array to contain only the values for which the
   * corresponding entry in `specs` is false.
   */
  def filter_array_not[T : ClassTag](data: Array[T], specs: Array[Boolean]) = {
    assert(data.size == specs.size)
    (data zip specs).filter(!_._2).map(_._1).toArray
  }

  /**
   * Determine which columns in the data don't vary at all depending on the
   * label. This means that, within an instance, the value for all labels is
   * the same. This happens primarily with columns referring to inherent
   * properties of the instance in question (in an instance that's a
   * query-candidate pair, such columns are features only of the query).
   * These columns aren't helpful in a model like this because, with only
   * one weight for all labels, and one value for all labels, the
   * corresponding term in the dot product will be the same for all labels
   * and hence the column has no discriminative power. In a perceptron
   * model, the weights for these columns generally end up zero. In R's
   * mlogit() function, such columns actually cause a singularity error.
   *
   * FIXME: Properly they should be specified separately as non-
   * choice-specific variables, with choice-specific weights. But that
   * requires more cleverness in handling the combination of choice-specific
   * and non-choice-specific weights, and in a reranking model it doesn't
   * make much sense to have choice-specific weights because the different
   * choices are in a sense equivalent to each other and generally have no
   * defining properties that can distinguish them, in the aggregate, from
   * other choices.
   */
  def check_singularity(data: Array[Array[Double]], nlabels: Int
  ): Array[Boolean] = {
    assert(data.size % nlabels == 0,
      "Size of data %s should be a multiple of %s" format(data.size, nlabels))
    // First, group by instance, check if all values in a given column
    // in the instance's data are the same. Result is an array of booleans
    // for each group, listing, for each column in the group, if all the
    // value are the same.
    val groups_same = data.
      grouped(nlabels).toArray.        // group by instance
      map { group =>                   // map over each group
        val head = group.head          // get first row
        group.transpose.               // array of rows -> array of cols
          zip(head).                   // zip col vals with first col val
          map { case (all, first) =>   // map over each zipped column
            all.forall(_ == first)     // true if all vals same as first
          }
      }

    // Then check for a given column if all groups have entirely the same
    // data across labels, i.e. all the values in a column are true. We
    // transpose to convert array of groups into array of columns so we
    // can easily map across columns, checking if all values in the column
    // are true.
    groups_same.transpose.map { col => col.forall(_ == true) }
  }

  /**
   * Convert the set of instances into a long-style data frame in R with
   * the name `framename`, for use with the `mlogit` function.
   */
  def do_mlogit(data: Iterable[(DI, Int)]) = {
    val R = RInScala()

    val agg_fv_data = data.map { case (di, label) =>
      val fv = di.feature_vector
      val aggfv = fv match {
        case agg: AggregateFeatureVector => agg
        case _ => internal_error("Only operates with AggregateFeatureVectors")
      }
      (aggfv, label)
    }

    val frame = "frame" // Name of variable to use for data, etc.
    val (headers, indiv, label, choice, data_rows) =
      AggregateFeatureVector.labeled_instances_to_R(agg_fv_data)
    val nlabel = agg_fv_data.head._1.depth
    val singular_columns = check_singularity(data_rows, nlabel)
    val singular_headers = filter_array(headers, singular_columns)
    val nonsingular_headers = filter_array_not(headers, singular_columns)
    val nonsingular_indices = filter_array_not((0 until headers.size).toArray,
      singular_columns)
    errprint(s"""Warning: Skipping non-choice-specific features: ${singular_headers mkString " "}""")
    val rheaders = nonsingular_headers.map {
      _.replaceAll("[^A-Za-z0-9_]", ".")
    }

    // errprint("#0")
    val rdata = data_rows map { row =>
      filter_array_not(row, singular_columns)
    }

    // errprint("#1")
    R.eval("require(mlogit)")
    // errprint("#2")
    R.update(s"$frame.headers", rheaders)
    // errprint("#3")
    R.update(s"$frame.indiv", indiv)
    // errprint("#4")
    R.update(s"$frame.label", label)
    // errprint("#5")
    R.update(s"$frame.choice", choice)
    // errprint("#6")
    R.update(s"$frame.data", rdata)
    // errprint("#7")
    val N = data_rows.size
    // errprint("#8")
    R.eval(s"dimnames($frame.data) = list(1:$N, $frame.headers)")
    // errprint("#9")
    R.eval(s"$frame = data.frame(indiv=$frame.indiv, label=$frame.label, choice=$frame.choice, $frame.data)")
    // errprint("#10")
    R.eval(s"""$frame = mlogit.data(choice="choice", shape="long", alt.var="label", chid.var="indiv", $frame)""")
    // errprint("#11")
    R.eval(s"""m.$frame = mlogit(choice ~ ${rheaders mkString " + "} | -1, $frame)""")
    // errprint("#12")
    val weights = R.toVector[Double](s"as.vector(m.$frame$$coefficients)")
    val expanded_buffer = mutable.Buffer.fill(headers.length)(0.0)
    for ((weight, index) <- (weights zip nonsingular_indices))
      expanded_buffer(index) = weight
    expanded_buffer.toArray
  }

  def get_weights(data: Iterable[(DI, Int)]): (VectorAggregate, Int) = {
    val rweights = do_mlogit(data)
    if (debug("weights"))
      errprint("Weights: %s", rweights)
    val vecagg = SingleVectorAggregate(ArrayVector(rweights))
    (vecagg, 1)
  }
}

