//  InterpretDTM.scala
//
//  Copyright (C) 2015 Ben Wing, The University of Texas at Austin
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
package postprocess

import util.argparser._
import util.collection._
import util.experiment._
import util.io.localfh
import util.print._
import util.table._

/**
 * See description under `InterpretDTM`.
 */
class InterpretDTMParameters(ap: ArgParser) {
  var output_dir = ap.option[String]("output-dir", "o", "od",
    help = """Directory or directories containing output from running DTM.""")

  var doc_file = ap.option[String]("doc-file", "d", "df",
    must = be_specified,
    help = """Document file containing info on the documents sent to DTM.""")

  var vocab_file = ap.option[String]("vocab-file", "v", "vf",
    must = be_specified,
    help = """Document file containing info on DTM vocabulary.""")

  var timeslice_file = ap.option[String]("timeslice-file", "t", "tf",
    must = be_specified,
    help = """Document file containing info on timeslices.""")

  var words_per_topic = ap.option[Int]("words-per-topic", "w", "wpt",
    default = 10,
    help = """Number of top words per topic to display.""")
}

/**
 * Interpret the output of DTM. The output is a directory, containing
 * a subdirectory 'lda-seq', containing a file 'info.dat' listing the
 * number of topics, terms and timeslices, as well as files
 * 'topic-###-var-e-log-prob.dat' containing the probabilities for each
 * term in each timeslice, in row-major order (the probabilities for all
 * timeslices for the first term, then the probabilities for all timeslices
 * for the second term, etc.).
 */
object InterpretDTM extends ExperimentApp("InterpretDTM") {

  type TParam = InterpretDTMParameters

  def create_param_object(ap: ArgParser) = new InterpretDTMParameters(ap)

  case class Document(title: String, coord: String, date: Int, counts: String)

  def read_log_probs(file: String, num_terms: Int, num_seq: Int) = {
    val log_probs = Array.fill(num_seq, num_terms)(0.0)
    for ((line, index) <- localfh.openr(file).zipWithIndex) {
      val seqind = index % num_seq
      val termind = index / num_terms
      log_probs(seqind)(termind) = line.toDouble
    }
    log_probs
  }

  def process_dir(dir: String) {
    // Read info file
    val info_file =
      localfh.openr("%s/lda-seq/info.dat" format params.output_dir)
    var num_topics = 0
    var num_terms = 0
    var seq_length = 0
    for (line <- info_file) {
      val parts = line.split(" ")
      if (parts.size == 2) {
        val Array(prop, value) = parts
        if (prop == "NUM_TOPICS")
          num_topics = value.toInt
        else if (prop == "NUM_TERMS")
          num_terms = value.toInt
        else if (prop == "SEQ_LENGTH")
          seq_length = value.toInt
      }
    }
    info_file.close()

    // Read vocab file
    val vocab = localfh.openr(params.vocab_file).toIndexedSeq

    // Read timeslice file; ignore first slice since we don't get
    // probabilities for it
    val timeslices = localfh.openr(params.timeslice_file).toIndexedSeq.tail

    // For each topic, find the top words
    for (topic <- 0 until num_topics) {
      // Read the probabilities; we get an array of arrays, first indexed
      // by timeslice, then by word
      val log_probs = read_log_probs("%s/lda-seq/topic-%03d-var-e-log-prob.dat"
        format (params.output_dir, topic), num_terms, seq_length)
      // For each timeslice, find the top N words by probability.
      // Transpose the resulting array of arrays so we output the timeslice
      // words in columns.
      val seq_top_words =
        log_probs.map { seq_topic_probs =>
          seq_topic_probs.zipWithIndex.sortBy(-_._1).
            take(params.words_per_topic).map(_._2).map {
              index => vocab(index)
            }.toIndexedSeq
        }.toIndexedSeq.transpose
      outprint("For dir %s, topic %s:" format (dir, topic))
      outprint(format_table(timeslices +: seq_top_words))
    }
  }

  def run_program(args: Array[String]) = {
    process_dir(params.output_dir)
    0
  }
}
