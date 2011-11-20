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

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.MeteredTask

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class ConvertTextToUnigramCountsParameters(parser: ArgParser) extends
    ProcessFilesParameters(parser) {
  var group_by_user =
    ap.flag("group-by-user",
      help = """If true, group tweets into a single per-user document.""")
}

class ConvertTextToUnigramCountsDriver extends ProcessFilesDriver {
  type ParamType = ConvertTextToUnigramCountsParameters

  val tweets_per_user = primmapmap[String, String, Int]()

  def usage() {
    sys.error("""Usage: ConvertTextToUnigramCounts [-o OUTDIR | --outfile OUTDIR] [--group-by-user] INFILE ...

Convert input files from raw-text format (one document per line) into unigram
counts, in the format expected by TextGrounder.  OUTDIR is the directory to
store the results in, which must not exist already.  If --group-by-user is
given, a document is the concatenation of all tweets for a given user.
Else, each individual tweet is a document.
""")
  }

  override def run_after_setup() {
    // iterate_input_files(group_by_user _)
    iterate_input_files(process_one_file _)
  }
  def process_one_file(lines: Iterator[String], outname: String) {
    val task = new MeteredTask("document", "processing")
    val out_counts_name = "%s/%s-counts-only-coord-documents.txt" format (params.output_dir, outname)
    errprint("Counts output file is %s..." format out_counts_name)
    val outstream = filehand.openw(out_counts_name, compression = "bzip2")
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
        }
      }
      task.item_processed()
    }
    outstream.close()
    task.finish()
  }
}

object ConvertTextToUnigramCounts extends
    ExperimentDriverApp("Convert raw text to unigram counts") {
  type DriverType = ConvertTextToUnigramCountsDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}
