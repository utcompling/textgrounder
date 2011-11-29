///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.preprocess

import collection.mutable

// import com.nicta.scoobi._
// import com.nicta.scoobi.Scoobi._
// import com.nicta.scoobi.io.text._
// import com.nicta.scoobi.io.text.TextInput._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.experiment.ExperimentDriverApp
import opennlp.textgrounder.util.ioutil.FileHandler

import opennlp.textgrounder.geolocate.GeoDocument

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class ConvertTextToUnigramCountsParameters(ap: ArgParser) extends
    ProcessCorpusParameters(ap) {
}

class ConvertTextToUnigramCountsFileProcessor(
  input_suffix: String, output_filehand: FileHandler,
  params: ConvertTextToUnigramCountsParameters
) extends ProcessCorpusFileProcessor(
  input_suffix, "unigram-counts",
  output_filehand, params.output_dir
) {
  def frob_row(fieldvals: Seq[String]) = {
    val docparams = mutable.LinkedHashMap[String, String]()
    docparams ++= (schema zip fieldvals)
    val text = docparams("text")
    docparams -= "text"
    val counts = intmap[String]()
    for (word <- text.split(" ", -1))
      counts(word) += 1
    val counts_text =
      (for ((word, count) <- counts.toSeq sortWith (_._2 > _._2)) yield
        ("%s:%s" format (GeoDocument.encode_word_for_counts_field(word),
          count))) mkString " "
    docparams += (("counts", counts_text))
    docparams.toSeq
  }
}

class ConvertTextToUnigramCountsDriver extends
    ProcessCorpusDriver {
  type ParamType = ConvertTextToUnigramCountsParameters

  def create_file_processor(input_suffix: String) =
    new ConvertTextToUnigramCountsFileProcessor(input_suffix, filehand, params)

  def get_input_corpus_suffix = "text"
}

//class ScoobiConvertTextToUnigramCountsDriver extends
//    BaseConvertTextToUnigramCountsDriver {
//
//  def usage() {
//    sys.error("""Usage: ScoobiConvertTextToUnigramCounts [-o OUTDIR | --outfile OUTDIR] [--group-by-user] INFILE ...
//
//Using Scoobi (front end to Hadoop), convert input files from raw-text format
//(one document per line) into unigram counts, in the format expected by
//TextGrounder.  OUTDIR is the directory to store the results in, which must not
//exist already.  If --group-by-user is given, a document is the concatenation
//of all tweets for a given user.  Else, each individual tweet is a document.
//""")
//  }
//
//  def process_files(filehand: FileHandler, files: Seq[String]) {
//    val task = new MeteredTask("tweet", "processing")
//    var tweet_lineno = 0
//    val task2 = new MeteredTask("user", "processing")
//    var user_lineno = 0
//    val out_counts_name = "%s/counts-only-coord-documents.txt" format (params.output_dir)
//    errprint("Counts output file is %s..." format out_counts_name)
//    val tweets = extractFromDelimitedTextFile("\t", params.files(0)) {
//      case user :: title :: text :: Nil => (user, text)
//    }
//    val counts = tweets.groupByKey.map {
//      case (user, tweets) => {
//        val counts = intmap[String]()
//        tweet_lineno += 1
//        for (tweet <- tweets) {
//          val words = tweet.split(' ')
//          for (word <- words)
//            counts(word) += 1
//        }
//        val result =
//          (for ((word, count) <- counts) yield "%s:%s" format (word, count)).
//            mkString(" ")
//        (user, result)
//      }
//    }
//
//    // Execute everything, and throw it into a directory
//    DList.persist (
//      TextOutput.toTextFile(counts, out_counts_name)
//    )
//  }
//}

object ConvertTextToUnigramCounts extends
    ExperimentDriverApp("ConvertTextToUnigramCounts") {
  type DriverType = ConvertTextToUnigramCountsDriver

  override def description =
"""Convert a corpus from raw-text format to unigram-counts format.  OUTDIR is
the directory to store the output corpus in, which must not exist already.
INDIR is the directory of the input corpus.
"""

  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}

//abstract class ScoobiApp(
//  progname: String
//) extends ExperimentDriverApp(progname) {
//  override def main(orig_args: Array[String]) = withHadoopArgs(orig_args) {
//    args => {
//      /* Thread execution back to the ExperimentDriverApp.  This will read
//         command-line arguments, call initialize_parameters() to verify
//         and canonicalize them, and then pass control back to us by
//         calling run_program(), which we override. */
//      set_errout_prefix(progname + ": ")
//      implement_main(args)
//    }
//  }
//}
//
//object ScoobiConvertTextToUnigramCounts extends
//    ScoobiApp("Convert raw text to unigram counts") {
//  type DriverType = ScoobiConvertTextToUnigramCountsDriver
//  def create_param_object(ap: ArgParser) = new ParamType(ap)
//  def create_driver() = new DriverType
//}

