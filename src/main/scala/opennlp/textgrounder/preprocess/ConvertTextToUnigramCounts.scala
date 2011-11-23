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

import java.io.InputStream

import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import com.nicta.scoobi.io.text.TextInput._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.MeteredTask

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class ConvertTextToUnigramCountsParameters(ap: ArgParser) extends
    ArgParserParameters(ap) {
  val output_dir =
    ap.option[String]("o", "output-dir",
      metavar = "DIR",
      help = """Directory to store output files in.""")
  val group_by_user =
    ap.flag("group-by-user",
      help = """If true, group tweets into a single per-user document.""")
  val files =
    ap.multiPositional[String]("files",
      help = """File(s) to process for input.""")
}

class ConvertTextToUnigramCountsFileProcessor(
  params: ConvertTextToUnigramCountsParameters
) extends TextFileProcessor {
  def process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) = {
    val compression_type = "bzip2"
    val task = new MeteredTask("document", "processing")
    val (_, basename) = filehand.split_filename(realname)
    val out_basename = basename.replaceAll("-[^-]*$", "") + "-counts.txt"
    val outname = filehand.join_filename(params.output_dir, out_basename)
    errprint("Counts output file is %s..." format outname)
    val outstream = filehand.openw(outname, compression = compression_type)

    var lineno = 0
    for (line <- lines) {
      val counts = intmap[String]()
      lineno += 1
      line.split("\t", -1).toList match {
        case title :: text :: Nil => {
          outstream.println("Article title: %s" format title)
          val words = text.split(' ')
          for (word <- words)
            counts(word) += 1
          output_reverse_sorted_table(counts, outstream)
        }
        case _ => {
          errprint("Bad line #%d: %s" format (lineno, line))
          errprint("Line length: %d" format line.split("\t", -1).length)
        }
      }
      task.item_processed()
    }
    outstream.close()
    task.finish()
    true
  }
}

abstract class BaseConvertTextToUnigramCountsDriver extends
    ArgParserExperimentDriver {
  type ParamType = ConvertTextToUnigramCountsParameters
  type RunReturnType = Unit
  
  val filehand = new LocalFileHandler

  val tweets_per_user = primmapmap[String, String, Int]()

  def handle_parameters() {
    need(params.output_dir, "output-dir")
  }

  def setup_for_run() { }

  def run_after_setup() {
    if (!filehand.make_directories(params.output_dir))
      param_error("Output dir %s must not already exist" format
        params.output_dir)

    process_files(filehand, params.files)
  }

  def process_files(filehand: FileHandler, files: Seq[String])
}

class ConvertTextToUnigramCountsDriver extends
    BaseConvertTextToUnigramCountsDriver {

  def usage() {
    sys.error("""Usage: ConvertTextToUnigramCounts [-o OUTDIR | --outfile OUTDIR] [--group-by-user] INFILE ...

Convert input files from raw-text format (one document per line) into unigram
counts, in the format expected by TextGrounder.  OUTDIR is the directory to
store the results in, which must not exist already.  If --group-by-user is
given, a document is the concatenation of all tweets for a given user.
Else, each individual tweet is a document.
""")
  }

  def process_files(filehand: FileHandler, files: Seq[String]) {
    new ConvertTextToUnigramCountsFileProcessor(params).
      process_files(filehand, files)
  }
}

class ScoobiConvertTextToUnigramCountsDriver extends
    BaseConvertTextToUnigramCountsDriver {

  def usage() {
    sys.error("""Usage: ScoobiConvertTextToUnigramCounts [-o OUTDIR | --outfile OUTDIR] [--group-by-user] INFILE ...

Using Scoobi (front end to Hadoop), convert input files from raw-text format
(one document per line) into unigram counts, in the format expected by
TextGrounder.  OUTDIR is the directory to store the results in, which must not
exist already.  If --group-by-user is given, a document is the concatenation
of all tweets for a given user.  Else, each individual tweet is a document.
""")
  }

  def process_files(filehand: FileHandler, files: Seq[String]) {
    val task = new MeteredTask("tweet", "processing")
    var tweet_lineno = 0
    val task2 = new MeteredTask("user", "processing")
    var user_lineno = 0
    val out_counts_name = "%s/counts-only-coord-documents.txt" format (params.output_dir)
    errprint("Counts output file is %s..." format out_counts_name)
    val tweets = extractFromDelimitedTextFile("\t", params.files(0)) {
      case user :: title :: text :: Nil => (user, text)
    }
    val counts = tweets.groupByKey.map {
      case (user, tweets) => {
        val counts = intmap[String]()
        tweet_lineno += 1
        for (tweet <- tweets) {
          val words = tweet.split(' ')
          for (word <- words)
            counts(word) += 1
        }
        val result =
          (for ((word, count) <- counts) yield "%s:%s" format (word, count)).
            mkString(" ")
        (user, result)
      }
    }

    // Execute everything, and throw it into a directory
    DList.persist (
      TextOutput.toTextFile(counts, out_counts_name)
    )
  }
}
object ConvertTextToUnigramCounts extends
    ExperimentDriverApp("Convert raw text to unigram counts") {
  type DriverType = ConvertTextToUnigramCountsDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}

abstract class ScoobiApp(
  progname: String
) extends ExperimentDriverApp(progname) {
  override def main(orig_args: Array[String]) = withHadoopArgs(orig_args) {
    args => {
      /* Thread execution back to the ExperimentDriverApp.  This will read
         command-line arguments, call initialize_parameters() to verify
         and canonicalize them, and then pass control back to us by
         calling run_program(), which we override. */
      set_errout_prefix(progname + ": ")
      implement_main(args)
    }
  }
}

object ScoobiConvertTextToUnigramCounts extends
    ScoobiApp("Convert raw text to unigram counts") {
  type DriverType = ScoobiConvertTextToUnigramCountsDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}

