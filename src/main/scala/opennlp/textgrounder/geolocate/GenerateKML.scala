///////////////////////////////////////////////////////////////////////////////
//  GenerateKML.scala
//
//  Copyright (C) 2010-2014 Ben Wing, The University of Texas at Austin
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

import java.io.{FileSystem=>_,_}

import org.apache.hadoop.io._

import util.argparser._
import util.spherical.SphereCoord
import util.experiment._
import util.error.warning
import util.print.errprint

import gridlocate._

import langmodel._

object KMLConstants {
  // FIXME: Allow these to be specified by command-line options
  // Minimum and maximum colors
  val kml_mincolor = Array(255.0, 255.0, 0.0) // yellow
  val kml_maxcolor = Array(255.0, 0.0, 0.0) // red

  // Params for starting place to look at.
  val look_at_latitude = 42
  val look_at_longitude = -102
  val look_at_altitude = 0
  val look_at_range = 5000000
  val look_at_tilt = 53.454348562403
  val look_at_heading = 0

  val min_lod_pixels = 16
}

class KMLParameters {
  // FIXME! Don't hard-code.
  val look_at_latitude = 42
  val look_at_longitude = -102
  val look_at_altitude = 0
  val look_at_range = 5000000
  val look_at_tilt = 53.454348562403
  val look_at_heading = 0

  var kml_max_height: Double = _

  var kml_transform: String = _

  var kml_include_cell_names: Boolean = _
}

class GenerateKMLParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
  //// Options used only in KML generation (--mode=generate-kml)
  var kml_words =
    ap.option[String]("k", "kml-words", "kw",
      must = be_specified,
      help = """Words to generate KML distributions for, when
--mode=generate-kml.  Each word should be separated by a comma.  A separate
file is generated for each word, using the value of '--kml-prefix' and adding
'.kml' to it.""")
  // Same as above but a sequence
  var split_kml_words =
    if (ap.parsedValues) kml_words.split(',')
    else Array[String]()
  var kml_prefix =
    ap.option[String]("kml-prefix", "kp",
      default = "kml-dist.",
      help = """Prefix to use for KML files outputted in --mode=generate-kml.
The actual filename is created by appending the word, and then the suffix
'.kml'.  Default '%default'.""")
  var kml_transform =
    ap.option[String]("kml-transform", "kt", "kx",
      default = "none",
      choices = Seq("none", "log", "logsquared"),
      help = """Type of transformation to apply to the probabilities
when generating KML (--mode=generate-kml), possibly to try and make the
low values more visible.  Possibilities are 'none' (no transformation),
'log' (take the log), and 'logsquared' (negative of squared log).  Default
'%default'.""")
  var kml_max_height =
    ap.option[Double]("kml-max-height", "kmh",
      default = 2000000.0,
      must = be_>(0.0),
      help = """Height of highest bar, in meters.  Default %default.""")
  var kml_include_cell_names =
    ap.flag("kml-include-cell-names", "kicn", "kml-names",
      help = """Include name of each cell in KML. Name comes from
most salient document within cell.""")
}

class GenerateKMLDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = GenerateKMLParameters
  type TRunRes = Unit

  override protected def get_lang_model_builder_creator(lm_type: String,
      word_weights: collection.Map[Gram, Double],
      missing_word_weight: Double
    ) = {
    if (lm_type != "unigram")
      param_error("Only unigram language models supported with GenerateKML")
    (factory: LangModelFactory) =>
      new FilterUnigramLangModelBuilder(
        factory, params.split_kml_words, !params.preserve_case_words,
        the_stopwords, the_whitelist, params.minimum_word_count,
        word_weights, missing_word_weight)
  }

  /**
   * Do the actual KML generation.  Some tracking info written to stderr.
   * KML files created and written on disk.
   */

  def run() {
    val grid = initialize_grid
    val cdist_factory = new CellDistFactory[SphereCoord]
    for (word <- params.split_kml_words) {
      val gram = Unigram.to_index(word)
      val celldist = cdist_factory.get_cell_dist(grid, gram)
      if (!celldist.normalized) {
        warning("""Non-normalized distribution, apparently word %s not seen anywhere.
Not generating an empty KML file.""", word)
      } else {
        val kmlparams = new KMLParameters()
        kmlparams.kml_max_height = params.kml_max_height
        kmlparams.kml_transform = params.kml_transform
        kmlparams.kml_include_cell_names = params.kml_include_cell_names
        SphereCellDist.generate_kml_file(grid, gram, celldist,
          "%s%s.kml" format (params.kml_prefix, word),
          kmlparams)
      }
    }
  }
}

object GenerateKML extends GeolocateApp("GenerateKML") {
  type TDriver = GenerateKMLDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

