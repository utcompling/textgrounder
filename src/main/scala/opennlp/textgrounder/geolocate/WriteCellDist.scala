///////////////////////////////////////////////////////////////////////////////
//  WriteCellDist.scala
//
//  Copyright (C) 2010-2013 Ben Wing, The University of Texas at Austin
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
package geolocate

import util.argparser._
import util.spherical.SphereCoord
import util.experiment._
import util.print.warning
import util.textdb._
 
import gridlocate._

import langmodel._
import LangModel._

class WriteCellDistParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
  var words =
    ap.option[String]("words",
      help = """Words to write distributions for.  Each word should be
separated by a comma.  For each word, a textdb file is written using the
value of `--output` as a prefix.""")
  // Same as above but a sequence
  var split_words:Seq[String] = _
  var output =
    ap.option[String]("o", "output",
      metavar = "FILE",
      help = """File prefix of written-out distributions.  Distributions are
stored as textdb corpora, i.e. for each word, two files will be written, formed
by adding `WORD.data.txt` and `WORD.schema.txt` to the prefix, with the former
storing the data as tab-separated fields and the latter naming the fields.""")
}


class WriteCellDistDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = WriteCellDistParameters
  type TRunRes = Unit

  override def handle_parameters() {
    super.handle_parameters()
    need(params.words, "words")
    params.split_words = params.words.split(',')
  }

  override protected def get_lang_model_builder_creator(lm_type: String) = {
    if (lm_type != "unigram")
      param_error("Only unigram language models supported with WriteCellDist")
    (factory: LangModelFactory) =>
      new FilterUnigramLangModelBuilder(
        factory,
        params.split_words,
        ignore_case = !params.preserve_case_words,
        stopwords = the_stopwords,
        whitelist = the_whitelist,
        minimum_word_count = params.minimum_word_count)
  }

  /**
   * Do the actual cell-dist generation.  Some tracking info written to stderr.
   */

  def run() {
    val grid = initialize_grid
    val cdist_factory = new CellDistFactory[SphereCoord](params.lru_cache_size)
    for (word <- params.split_words) {
      val celldist = cdist_factory.get_cell_dist(grid, memoizer.memoize(word))
      if (!celldist.normalized) {
        warning("""Non-normalized distribution, apparently word %s not seen anywhere.
Not generating a cell-distribution file.""", word)
      } else {
        val base = params.output + word
        val cellprob_props =
          for ((cell, prob) <- celldist.cellprobs) yield {
            cell.to_row ++ Seq(
              ("probability", prob)
            )
          }
        note_result("textdb-type", "textgrounder-cell-dist")
        note_result("cell-dist-word", Encoder.string(word))
        write_textdb_values_with_results(util.io.localfh, base,
          cellprob_props.iterator)
      }
    }
  }
}

object WriteCellDist extends GeolocateApp("WriteCellDist") {
  type TDriver = WriteCellDistDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

