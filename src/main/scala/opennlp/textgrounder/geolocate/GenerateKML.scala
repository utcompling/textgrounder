///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010, 2011 Ben Wing, The University of Texas at Austin
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

////////
//////// GenerateKML.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import java.io.{FileSystem=>_,_}

import org.apache.hadoop.io._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.printutil.{errprint, warning}

import WordDist.memoizer._
import GenericTypes._

class KMLParameters {
  // Minimum and maximum colors
  // FIXME: Allow these to be specified by command-line options
  val kml_mincolor = Array(255.0, 255.0, 0.0) // yellow
  val kml_maxcolor = Array(255.0, 0.0, 0.0) // red

  var kml_max_height: Double = _

  var kml_transform: String = _
}

class GenerateKMLParameters(
  parser: ArgParser = null
) extends GeolocateParameters(parser) {
  //// Options used only in KML generation (--mode=generate-kml)
  var kml_words =
    ap.option[String]("k", "kml-words", "kw",
      help = """Words to generate KML distributions for, when
--mode=generate-kml.  Each word should be separated by a comma.  A separate
file is generated for each word, using the value of '--kml-prefix' and adding
'.kml' to it.""")
  // Same as above but a sequence
  var split_kml_words:Seq[String] = _
  var split_kml_words_memoized:Seq[Word] = _
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
      help = """Height of highest bar, in meters.  Default %default.""")
}


/* A factory that filters the distributions to contain only the words we
   care about, to save memory and time. */
class FilterPseudoGoodTuringSmoothedWordDistFactory(
    filter_words: Seq[Word]
  ) extends PseudoGoodTuringSmoothedWordDistFactory {
  val oov = memoize_string("-OOV-")
  override def set_unigram_word_dist(doc: GenericDistDocument,
      keys: Array[Word], values: Array[Int], num_words: Int,
      is_training_set: Boolean) {
    val (newkeys, newvalues) =
      (for ((k, v) <- (keys zip values).take(num_words);
           newk = if (filter_words contains k) k else oov)
         yield (k, v)).unzip
    doc.dist = new PseudoGoodTuringSmoothedWordDist(this,
        newkeys.toArray, newvalues.toArray, newkeys.length,
        note_globally = is_training_set)
  }
}

class WordCellTupleWritable extends
    WritableComparable[WordCellTupleWritable] {
  var word: String = _
  var index: RegularCellIndex = _

  def set(word: String, index: RegularCellIndex) {
    this.word = word
    this.index = index
  }

  def write(out: DataOutput) {
    out.writeUTF(word)
    out.writeInt(index.latind)
    out.writeInt(index.longind)
  }

  def readFields(in: DataInput) {
    word = in.readUTF()
    val latind = in.readInt()
    val longind = in.readInt()
    index = RegularCellIndex(latind, longind)
  }

  // It hardly matters how we compare the cell indices.
  def compareTo(other: WordCellTupleWritable) =
    word.compareTo(other.word)
}

class GenerateKMLDriver extends
    GeolocateDriver with StandaloneGeolocateDriverStats {
  type TParam = GenerateKMLParameters
  type TRunRes = Unit

  override def handle_parameters() {
    super.handle_parameters()
    need(params.kml_words, "kml-words")
    params.split_kml_words = params.kml_words.split(',')
    params.split_kml_words_memoized =
      for (w <- params.split_kml_words) yield memoize_string(w)
  }

  override def initialize_word_dist_suffix() = 
    DistDocument.unigram_counts_suffix
  override def initialize_word_dist_factory() = {
    new FilterPseudoGoodTuringSmoothedWordDistFactory(
      params.split_kml_words_memoized)
  }

  /**
   * Do the actual KML generation.  Some tracking info written to stderr.
   * KML files created and written on disk.
   */

  def run_after_setup() {
    val cdist_factory = new SphereCellDistFactory(params.lru_cache_size)
    for (word <- params.split_kml_words) {
      val celldist = cdist_factory.get_cell_dist(cell_grid, memoize_string(word))
      if (!celldist.normalized) {
        warning("""Non-normalized distribution, apparently word %s not seen anywhere.
Not generating an empty KML file.""", word)
      } else {
        val kmlparams = new KMLParameters()
        kmlparams.kml_max_height = params.kml_max_height
        kmlparams.kml_transform = params.kml_transform
        celldist.generate_kml_file("%s%s.kml" format (params.kml_prefix, word),
          kmlparams)
      }
    }
  }
}

object GenerateKMLApp extends GeolocateApp("generate-kml") {
  type TDriver = GenerateKMLDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver()
}

