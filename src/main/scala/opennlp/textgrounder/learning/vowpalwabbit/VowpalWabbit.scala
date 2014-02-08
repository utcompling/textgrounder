///////////////////////////////////////////////////////////////////////////////
//  VowpalWabbit.scala
//
//  Copyright (C) 2014 Ben Wing, The University of Texas at Austin
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
package learning.vowpalwabbit

import scala.sys.process._

import learning._
import util.debug._
import util.io.localfh
import util.metering._
import util.print.errprint

/**
 * This implements an interface onto the Vowpal Wabbit machine learning
 * package.
 *
 * Vowpal Wabbit assumes the following format for data in the
 * non-label-specific format:
 *
 * Each data instance consists of one line: A class (pretty much any string
 * without a colon, | or space in it), followed by a |, a space, and
 * either features or feature:value pairs, separated by spaces, e.g.
 *
 * 1 | height:1.5 length:2.0 NumberOfLegs:4.0 HasStripes:1.0
 *
 * It's also possible to group features into "namespaces", which is
 * used for presentation purposes mainly (note the lack of space after
 * the pipe symbol):
 *
 *1 |MetricFeatures height:1.5 length:2.0 |OtherFeatures NumberOfLegs:4.0 HasStripes:1.0
 *
 * There's also a cost-sensitive format (with option -csoaa or -wap,
 * standing for "cost-sensitive one against all" or "weighted all pairs"):
 *
 * 1:0 2:3 3:1.5 4:1 | foo:1 bar:2 baz bat
 *
 * In this example, there are four labels, with given costs. The correct
 * label is #1, with 0 cost.
 *
 * There's also a cost-sensitive label-dependent format (with option
 * -csoaa_ldf or -wap_ldf, which takes an argument that should be either
 * "multiline" or "singleline"; presumably "multiline" is what's described
 * below). See this post from Hal Daume:
 *
 * https://groups.yahoo.com/neo/groups/vowpal_wabbit/conversations/topics/626
 *
 * Hal gives the following cost-sensitive example:
 *
 * 1:1.0 2:0.0 3:2.0 | a b c
 * 1:1.0 2:0.0 | b c d
 * 1:1.0 3:2.0 | a b c
 *
 * and the equivalent in label-dependent format:
 *
 * 1 1.0 | a_1 b_1 c_1
 * 2 0.0 | a_2 b_2 c_2
 * 3 2.0 | a_3 b_3 c_3
 *
 * 1 1.0 | b_1 c_1 d_1
 * 2 0.0 | b_2 c_2 d_2
 *
 * 1 1.0 | a_1 b_1 c_1
 * 3 2.0 | a_3 b_3 c_3
 *
 * It's not necessary to memoize features to indices, and might be better
 * not to, because VW will do that for you using feature hashing.
 */

/**
 * Train a classifying model using VowpalWabbit.
 */
class VowpalWabbitTrainer(
  val gaussian: Double,
  val lasso: Double
) {
  def write_feature_file(
      features: Iterator[(String, Iterable[(String, Double)])],
      file: String
  ) {
    errprint(s"Writing features to $file")
    val task = new Meter("processing features in", "document")

    val f = localfh.openw(file)
    features.foreachMetered(task) { case (correct, feats) =>
      if (debug("features")) {
        val prefix = "#%s" format (task.num_processed + 1)
        feats.foreach { case (feat, value) =>
          errprint(s"  $prefix: $feat = $value")
        }
      }
      val line = s"$correct | " +
        feats.map { case (feat, value) =>
          val goodfeat = feat.replace(":", "_")
          s"$goodfeat:$value"
        }.mkString(" ")
      f.println(line)
    }
    f.close()
  }

  def train(features: Iterator[(String, Iterable[(String, Double)])]) = {
    var feats_filename =
      java.io.File.createTempFile("textgrounder.vw.feats", null).toString
    write_feature_file(features, feats_filename)
    val cache_filename =
      java.io.File.createTempFile("textgrounder.vw.cache", null).toString
    val model_filename =
      java.io.File.createTempFile("textgrounder.vw.model", null).toString
    errprint(s"Writing VW cache to $cache_filename")
    errprint(s"Writing VW model to $model_filename")
    val vw_cmd_line_start =
      Seq("vw", "--cache-file", cache_filename, "--data", feats_filename,
          "-f", model_filename, "--compressed")
    val vw_penalty =
      (if (gaussian > 0) Seq("--l2", s"$gaussian") else Seq()) ++
      (if (lasso > 0) Seq("--l1", s"$lasso") else Seq())
    time_action("running VowpalWabbit") {
      (vw_cmd_line_start ++ vw_penalty) !
    }
  }
}
