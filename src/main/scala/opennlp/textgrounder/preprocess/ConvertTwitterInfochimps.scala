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
18) Empty?


3) We want to convert each to two files: one containing the article-data,
   one containing the text.  We can later convert the text to unigram counts,
   bigram counts, etc.

*/

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

class ConvertTwitterInfochimpsParameters(parser: ArgParser) extends
    ProcessFilesParameters(parser) {
  var group_by_user =
    ap.flag("group-by-user",
      help = """If true, prepare for grouping tweets into a single per-user document.""")
}

class ConvertTwitterInfochimpsDriver extends ProcessFilesDriver {
  type ParamType = ConvertTwitterInfochimpsParameters
  
  def usage() {
    sys.error("""Usage: ConvertTwitterInfochimps [-o OUTDIR | --outfile OUTDIR] [--group-by-user] INFILE ...

Convert input files in the Infochimps Twitter corpus into files in the
format expected by TextGrounder.  OUTDIR is the directory to store the
results in, which must not exist already.  If --group-by-user is given,
output an extra field in the text file containing the user, to make it
easier to group tweets by user.
""")
  }

  override def run_after_setup() {
    val fields = List("id", "title", "split", "coord", "time",
      "username", "userid", "reply_username", "reply_userid", "anchor", "lang")
    // FIXME: Output fields as a schema file
    super.run_after_setup()
  }

  def process_one_file(lines: Iterator[String], outname: String) {
    val compression_type = "bzip2"
    val task = new MeteredTask("tweet", "parsing")
    val out_metadata_name =
      "%s/%s-combined-document-data.txt" format (params.output_dir, outname)
    val out_text_name = "%s/%s-text.txt" format (params.output_dir, outname)
    errprint("Metadata output file is %s..." format out_metadata_name)
    errprint("Text output file is %s..." format out_text_name)
    val outstream_metadata = filehand.openw(out_metadata_name,
      compression = compression_type)
    val outstream_text = filehand.openw(out_text_name,
      compression = compression_type)
    var lineno = 0
    for (line <- lines) {
      lineno += 1
      line.split("\t", -1).toList match {
        case id :: time :: userid :: username :: _ ::
            reply_username :: reply_userid :: _ :: text :: anchor :: lang ::
            lat :: long :: _ :: _ :: _ :: _ :: _ :: Nil => {
          val title = "Twitter:" + id
          val metadata = List(id, title, "training",
            "%s,%s" format (lat, long), time, username, userid,
            reply_username, reply_userid, anchor, lang)
          outstream_metadata.println(metadata mkString "\t")
          val rawtext = unescapeHtml4(unescapeXml(text))
          val splittext = Twokenize(rawtext)
          val textdata =
            if (params.group_by_user)
              List(username, title, splittext mkString " ")
            else
              List(title, splittext mkString " ")
          outstream_text.println(textdata mkString "\t")
        }
        case _ => {
          errprint("Bad line #%d: %s" format (lineno, line))
          errprint("Line length: %d" format line.split("\t", -1).length)
        }
      }
      task.item_processed()
    }
    outstream_text.close()
    outstream_metadata.close()
    task.finish()
  }
}

object ConvertTwitterInfochimps extends
    ExperimentDriverApp("Convert Twitter Infochimps") {
  type DriverType = ConvertTwitterInfochimpsDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}
