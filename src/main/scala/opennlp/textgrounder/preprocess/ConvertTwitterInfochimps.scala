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

import java.util.zip.GZIPInputStream

import org.apache.commons.lang3.StringEscapeUtils._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.Twokenize

/*

Steps for converting Infochimps to our format:

1) Input is a series of files, e.g. part-00000.gz, each about 180 MB.
2) Each line looks like this:


100000018081132545      20110807002716  25430513        GTheHardWay                                     Niggas Lost in the Sauce ..smh better slow yo roll and tell them hoes to get a job nigga #MRIloveRatsIcanchange&amp;amp;saveherassNIGGA &lt;a href=&quot;http://twitter.com/download/android&quot; rel=&quot;nofollow&quot;&gt;Twitter for Android&lt;/a&gt;    en      42.330165       -83.045913                                      
The fields are:

1) Tweet ID
2) Time
3) User ID
4) User name
5) Empty?
6) User name being replied to (FIXME: which JSON field is this?)
7) User ID for replied-to user name (but sometimes different ID's for same user name)
8) Empty?
9) Tweet text -- double HTML-encoded (e.g. & becomes &amp;amp;)
10) HTML anchor text indicating a link of some sort, HTML-encoded (FIXME: which JSON field is this?)
11) Language, as a two-letter code
12) Latitude
13) Longitude
14) Empty?
15) Empty?
16) Empty?
17) Empty?


3) We want to convert each to two files: (1) containing the article-data 

*/

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

/**
 * Class retrieving command-line arguments or storing programmatic
 * configuration parameters.
 *
 * @param parser If specified, should be a parser for retrieving the
 *   value of command-line arguments from the command line.  Provided
 *   that the parser has been created and initialized by creating a
 *   previous instance of this same class with the same parser (a
 *   "shadow field" class), the variables below will be initialized with
 *   the values given by the user on the command line.  Otherwise, they
 *   will be initialized with the default values for the parameters.
 *   Because they are vars, they can be freely set to other values.
 *
 */
class ConvertTwitterInfochimpsParameters(parser: ArgParser) extends
    ArgParserParameters(parser) {
  protected val ap = parser

  var output_dir =
    ap.option[String]("o", "output-dir",
      metavar = "DIR",
      help = """Directory to store output files in.""")

  var files =
    ap.multiPositional[String]("infile",
      help = """File(s) to process for input.""")
}

/**
 * Base class for programmatic access to document/etc. geolocation.
 * Subclasses are for particular apps, e.g. GeolocateDocumentDriver for
 * document-level geolocation.
 *
 * NOTE: Currently the code has some values stored in singleton objects,
 * and no clear provided interface for resetting them.  This basically
 * means that there can be only one geolocation instance per JVM.
 * By now, most of the singleton objects have been removed, and it should
 * not be difficult to remove the final limitations so that multiple
 * drivers per JVM (possibly not at the same time) can be done.
 *
 * Basic operation:
 *
 * 1. Create an instance of the appropriate subclass of GeolocateParameters
 * (e.g. GeolocateDocumentParameters for document geolocation) and populate
 * it with the appropriate parameters.  Don't pass in any ArgParser instance,
 * as is the default; that way, the parameters will get initialized to their
 * default values, and you only have to change the ones you want to be
 * non-default.
 * 2. Call run(), passing in the instance you just created.
 *
 * NOTE: Currently, some of the fields of the GeolocateParameters-subclass
 * are changed to more canonical values.  If this is a problem, let me
 * know and I'll fix it.
 *
 * Evaluation output is currently written to standard error, and info is
 * also returned by the run() function.  There are some scripts to parse the
 * console output.  See below.
 */
class ConvertTwitterInfochimpsDriver extends ArgParserExperimentDriver {
  type ParamType = ConvertTwitterInfochimpsParameters
  type RunReturnType = Unit

  val filehand = new LocalFileHandler
  
  def usage() {
    sys.error("""Usage: ConvertTwitterInfochimps INFILE OUTDIR

Convert input files in the Infochimps Twitter corpus into files in the
format expected by TextGrounder.  INFILE is a single file or a glob.
OUTDIR is the directory to store the results in.
""")
  }

  def handle_parameters() {
    need(params.output_dir, "output-dir")
    if (!filehand.is_directory(params.output_dir))
      param_error("Output dir %s need to name an existing directory" format
        params.output_dir)
  }

  def setup_for_run() { }

  def run_after_setup() {

    val fields = List("id", "title", "split", "coord", "time",
      "username", "userid", "reply_username", "reply_userid", "anchor", "lang")

    for (file <- params.files) {
      val task = new MeteredTask("tweet", "parsing")
      errprint("Processing %s..." format file)
      var (_, outname) = filehand.split_filename(file)
      val raw_instream = filehand.get_input_stream(file)
      val instream = {
        if (file.endsWith(".gz")) {
          outname = outname.stripSuffix(".gz")
          new GZIPInputStream(raw_instream)
        } else
          raw_instream
      }
      errprint("Processing %s..." format file)
      val out_metadata_name =
        "%s/%s-combined-document-data.txt" format (params.output_dir, outname)
      val out_text_name = "%s/%s-text.txt" format (params.output_dir, outname)
      errprint("Metadata output file is %s..." format out_metadata_name)
      errprint("Text output file is %s..." format out_text_name)
      val outstream_metadata = filehand.openw(out_metadata_name)
      val outstream_text = filehand.openw(out_text_name)
      var lineno = 0
      for (line <- new FileIterator(instream)) {
        lineno += 1
        line.split("\t", -1).toList match {
          case id :: time :: userid :: username :: _ ::
              reply_username :: reply_userid :: _ :: text :: anchor :: lang ::
              lat :: long :: _ :: _ :: _ :: _ => {
            val metadata = List(id, id, "training", "%s,%s" format (lat, long),
              time, username, userid, reply_username, reply_userid, anchor,
              lang)
            outstream_metadata.println(metadata mkString "\t")
            val rawtext = unescapeHtml4(unescapeXml(text))
            val splittext = Twokenize(rawtext)
            val textdata = List(id, id, splittext mkString " ")
            outstream_text.println(textdata mkString "\t")
          }
          case _ => {
            errprint("Bad line #%d: %s" format (lineno, line))
          }
        }
        task.item_processed()
      }
      outstream_text.close()
      outstream_metadata.close()
      task.finish()
    }
  }
}

object ConvertTwitterInfochimps extends
    ExperimentDriverApp("Convert Twitter Infochimps") {
  type DriverType = ConvertTwitterInfochimpsDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}
