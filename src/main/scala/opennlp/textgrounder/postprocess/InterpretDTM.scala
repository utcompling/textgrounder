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
    must = be_specified,
    help = """Directory or directories containing output from running DTM.""")

  var input_prefix = ap.option[String]("input-prefix", "i", "ip",
    must = be_specified,
    help = """Input prefix of files sent to DTM.""")

  var words_per_topic = ap.option[Int]("words-per-topic", "w", "wpt",
    default = 10,
    help = """Number of top words per topic to display.""")

  var latex = ap.flag("latex",
    help = """Display in LaTeX format.""")

  var rename_headings = ap.option[String]("rename-headings", "rh",
    help = """Rename headings, in the form FROM=TO,FROM=TO,....""")

  var topics = ap.option[String]("topics", "t",
    help = """Topics to display; numbers separated by commas.""")

  var boldface = ap.option[String]("boldface", "b",
    help = """Words to boldface. Format is WORD,WORD,... In place of
a WORD can be WORD/TOPIC where TOPIC is a topic number to boldface
only a word in a specific topic.""")

  var two_columns = ap.flag("two-columns", "tc",
    help = """Display topics as two columns.""")
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
      val termind = index / num_seq
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
    val vocab = localfh.openr(params.input_prefix + "-vocab.dat").toIndexedSeq

    // Read timeslice file
    val orig_timeslices = localfh.openr(params.input_prefix + "-slice.dat").toIndexedSeq

    // Compute map to remap headings
    val remap_headings =
      if (params.rename_headings == null) Map[String,String]()
      else params.rename_headings.split(",").map { _.split("=") }.map {
        case Array(from, to) => (from, to)
      }.toMap

    // Compute remapped headings
    val timeslices = orig_timeslices.map { x => remap_headings.getOrElse(x, x) }

    // Set of topics to include
    val topicset =
      if (params.topics == null) (0 until num_topics).toSet
      else params.topics.split(",").map(_.toInt).toSet

    // Words in topics to boldface
    var boldfacemap =
      if (params.boldface == null) Map[String, Int]()
      else {
        params.boldface.split(",").map { spec =>
          if (spec contains "/") {
            val Array(word, topic) = spec.split("/")
            (word, topic.toInt)
          } else
            (spec, -1)
        }.toMap
      }

    // For each topic, find the top words
    val topic_top_words =
      for (topic <- 0 until num_topics; if topicset contains topic) yield {
        // Read the probabilities; we get an array of arrays, first indexed
        // by timeslice, then by word
        val log_probs = read_log_probs("%s/lda-seq/topic-%03d-var-e-log-prob.dat"
          format (params.output_dir, topic), num_terms, seq_length)
        // outprint("Log probs: %s" format log_probs.map(_.toIndexedSeq).toIndexedSeq)
        // For each timeslice, find the top N words by probability.
        // Transpose the resulting array of arrays so we output the timeslice
        // words in columns.
        // val seq_top_word_probs =
        //   log_probs.map { seq_topic_probs =>
        //     seq_topic_probs.zipWithIndex.sortBy(-_._1).
        //       take(params.words_per_topic).map {
        //         case (prob, index) => "%s, %s" format (index, prob)
        //       }.toIndexedSeq
        //   }.toIndexedSeq.transpose
        // outprint(format_table(timeslices +: seq_top_word_probs))
        val seq_top_words =
         log_probs.map { seq_topic_probs =>
           seq_topic_probs.zipWithIndex.sortBy(-_._1).
             take(params.words_per_topic).map(_._2).map {
               index => vocab(index)
             }.toIndexedSeq
         }.toIndexedSeq.transpose
        (seq_top_words, topic)
      }

    // Maybe boldface some words
    val bf_topic_top_words =
      if (!params.latex) topic_top_words
      else {
        topic_top_words.map { case (words, topic) =>
          (words.map { line =>
            line.map { word =>
              val bftopic = boldfacemap.getOrElse(word, -2)
              if (bftopic == -1 || bftopic == topic)
                """\textbf{\textcolor{red}{%s}}""" format word
              else
                word
            }
          }, topic)
        }
      }

    def paste_two_topics(topic_1: Seq[Seq[String]],
        topic_2: Seq[Seq[String]]) = {
      (topic_1 zip topic_2).map { case (x, y) => x ++ y }
    }

    if (params.two_columns) {
      if (params.latex)
        outprint("""\begin{tabular}{|%s|%s}""", "c|" * timeslices.size,
          "c|" * timeslices.size)
      bf_topic_top_words.sliding(2, 2).foreach { group =>
        val ((tstr1, tstr2), headers, words) = group match {
          case Seq((tw1, topic1), (tw2, topic2)) => {
            (("Topic %s" format topic1, "Topic %s" format topic2),
             timeslices ++ timeslices,
             paste_two_topics(tw1, tw2))
          }
          case Seq((tw1, topic1)) => {
            (("Topic %s" format topic1, ""),
             timeslices ++ timeslices.map(x => ""),
             paste_two_topics(tw1, tw1.map { x => x.map(y => "") }))
          }
        }
        if (params.latex) {
          outprint("""\hline
\multicolumn{%s}{|c||}{%s} & \multicolumn{%s}{|c|}{%s} \\
\hline
%s \\
\hline
\hline""",
            timeslices.size, tstr1, timeslices.size, tstr2,
            headers mkString " & "
          )
          for (line <- words) {
            outprint("""%s \\
\hline""",
              line mkString " & "
            )
          }
        } else {
          outprint("For dir %s: %s, %s" format (dir, tstr1, tstr2))
          outprint(format_table(headers +: words))
        }
      }
      if (params.latex)
        outprint("""\end{tabular}""")
    } else {
      if (params.latex)
        outprint("""\begin{tabular}{|%s}""", "c|" * timeslices.size)
      for ((seq_top_words, topic) <- bf_topic_top_words) {
        if (params.latex) {
          outprint("""\hline
\multicolumn{%s}{|c|}{Topic %s} \\
\hline
%s \\
\hline
\hline""",
            timeslices.size, topic, timeslices mkString " & "
          )
          for (line <- seq_top_words) {
            outprint("""%s \\
\hline""",
              line mkString " & "
            )
          }
        } else {
          outprint("For dir %s, topic %s:" format (dir, topic))
          outprint(format_table(timeslices +: seq_top_words))
        }
      }
      if (params.latex)
        outprint("""\end{tabular}""")
    }
  }

  def run_program(args: Array[String]) = {
    process_dir(params.output_dir)
    0
  }
}
