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

import java.io.{InputStream, PrintStream}

import org.apache.commons.lang3.StringEscapeUtils._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil.{FileHandler, LocalFileHandler, TextFileProcessor}
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.printutil.{errprint, output_reverse_sorted_table, set_errout_prefix}
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

class ConvertTwitterInfochimpsParameters(ap: ArgParser) extends
    ArgParserParameters(ap) {
  val output_dir =
    ap.option[String]("o", "output-dir",
      metavar = "DIR",
      help = """Directory to store output files in.""")
  val output_stats =
    ap.flag("output-stats",
      help = """If true, output statistics on the tweets in the input,
rather than converting the files.""")
  val files =
    ap.multiPositional[String]("files",
      help = """File(s) to process for input.""")
}

abstract class TwitterInfochimpsFileProcessor extends TextFileProcessor {
  def process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) = {
    val task = new MeteredTask("tweet", "parsing")
    var lineno = 0
    for (line <- lines) {
      lineno += 1
      line.split("\t", -1).toList match {
        case id :: time :: userid :: username :: _ ::
            reply_username :: reply_userid :: _ :: text :: anchor :: lang ::
            lat :: long :: _ :: _ :: _ :: _ :: _ :: Nil => {
          val title = id
          val metadata = Seq(id, title, "training",
            "%s,%s" format (lat, long), time, username, userid,
            reply_username, reply_userid, anchor, lang)
          process_line(metadata, text)
        }
        case _ => {
          errprint("Bad line #%d: %s" format (lineno, line))
          errprint("Line length: %d" format line.split("\t", -1).length)
        }
      }
      task.item_processed()
    }
    task.finish()
    true
  }

  // To be implemented

  def process_line(metadata: Seq[String], text: String)
}

class ConvertTwitterInfochimpsFileProcessor(
  params: ConvertTwitterInfochimpsParameters
) extends TwitterInfochimpsFileProcessor {
  var outstream: PrintStream = _
  val compression_type = "bzip2"

  override def begin_process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) {
    val (_, outname) = filehand.split_filename(realname)
    val out_text_name = "%s/%s-text.txt" format (params.output_dir, outname)
    errprint("Text output file is %s..." format out_text_name)
    outstream = filehand.openw(out_text_name, compression = compression_type)
    super.begin_process_lines(lines, filehand, file, compression, realname)
  }

  def process_line(metadata: Seq[String], text: String) {
    val rawtext = unescapeHtml4(unescapeXml(text))
    val splittext = Twokenize(rawtext)
    val outdata = metadata ++ Seq(splittext mkString " ")
    outstream.println(outdata mkString "\t")
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    outstream.close()
    super.end_process_file(filehand, file)
  }
}

class TwitterInfochimpsStatsFileProcessor extends
    TwitterInfochimpsFileProcessor {
  class TwitterStatistics {
    // Over-all statistics
    var num_tweets = 0

    // Time-based statistics
    val tweets_by_day = intmap[String]()
    val tweets_by_hour = intmap[String]()
    val tweets_by_minute = intmap[String]()
    // In general, when finding the minimum, we want the default to be greater
    // than any possible value, and when finding the maximum, we want the
    // default to be less than any possible value.  For string comparison,
    // the empty string is less than any other string, and a string beginning
    // with Unicode 0xFFFF is greater than almost any other string. (0xFFFF
    // is not a valid Unicode character.)
    var earliest_tweet_time = "\uFFFF"
    var latest_tweet_time = ""
    var earliest_tweet_id = "\uFFFF"
    var latest_tweet_id = ""

    // If tweet ID's are sorted by time, the following two should be
    // identical to the earliest_tweet_id/latest_tweet_id.
    var lowest_tweet_id = "\uFFFF"
    var highest_tweet_id = ""

    // User-based statistics
    val tweets_by_user = intmap[String]()
    val userid_by_user = setmap[String, String]()
    val reply_userid_by_reply_user = setmap[String, String]()
    val reply_user_by_user = setmap[String, String]()

    def record_tweet(metadata: Seq[String], text: String) {
      assert(metadata.length == 11)
      val Seq(id, title, split, coords, time, username, userid, reply_username,
        reply_userid, anchor, lang) = metadata

      num_tweets += 1

      // Time format is YYYYMMDDHHmmSS, hence take(8) goes up through the day,
      // take(10) up through the hour, etc.
      tweets_by_day(time.take(8)) += 1
      tweets_by_hour(time.take(10)) += 1
      tweets_by_minute(time.take(12)) += 1

      if (time < earliest_tweet_time) {
        earliest_tweet_time = time
        earliest_tweet_id = id
      }
      if (time > latest_tweet_time) {
        latest_tweet_time = time
        latest_tweet_id = id
      }
      if (id < lowest_tweet_id)
        lowest_tweet_id = id
      if (id > highest_tweet_id)
        highest_tweet_id = id

      tweets_by_user(username) += 1
      userid_by_user(username) += userid
      if (reply_username != "") {
        reply_userid_by_reply_user(reply_username) += reply_userid
        reply_user_by_user(username) += reply_username
      }
    }

    def print_statistics() {
      errprint("Number of tweets: %s" format num_tweets)
      errprint("Earliest tweet: %s at %s" format
        (earliest_tweet_id, earliest_tweet_time))
      errprint("Lowest tweet ID: %s" format lowest_tweet_id)
      errprint("Latest tweet: %s at %s" format
        (latest_tweet_id, latest_tweet_time))
      errprint("Highest tweet ID: %s" format highest_tweet_id)
      errprint("Number of users: %s" format tweets_by_user.size)
      output_reverse_sorted_table(tweets_by_user)
    }
  }

  var curfile: String = _
  var curfile_stats: TwitterStatistics = _
  val global_stats = new TwitterStatistics

  override def begin_process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) {
    val (_, outname) = filehand.split_filename(realname)
    curfile = outname
    curfile_stats = new TwitterStatistics
    super.begin_process_lines(lines, filehand, file, compression, realname)
  }

  def process_line(metadata: Seq[String], text: String) {
    curfile_stats.record_tweet(metadata, text)
    global_stats.record_tweet(metadata, text)
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    errprint("Statistics for file %s:" format curfile)
    curfile_stats.print_statistics()
    super.end_process_file(filehand, file)
  }

  override def end_processing(filehand: FileHandler, files: Iterable[String]) {
    errprint("Statistics for all files:")
    global_stats.print_statistics()
  }
}

class ConvertTwitterInfochimpsDriver extends ArgParserExperimentDriver {
  type ParamType = ConvertTwitterInfochimpsParameters
  type RunReturnType = Unit
  
  val filehand = new LocalFileHandler
  
  def usage() {
    sys.error("""Usage: ConvertTwitterInfochimps [-o OUTDIR | --outfile OUTDIR] [--output-stats] INFILE ...

Convert input files in the Infochimps Twitter corpus into files in the
format expected by TextGrounder.  If --output-stats is given,
output statistics to stderr rather than converting text.  Otherwise,
store results in OUTDIR, which must not exist already.  
""")
  }

  def handle_parameters() {
    if (!params.output_stats)
      need(params.output_dir, "output-dir")
  }

  def setup_for_run() { }

  def run_after_setup() {
    if (params.output_stats)
      new TwitterInfochimpsStatsFileProcessor().
        process_files(filehand, params.files)
    else {
      if (!filehand.make_directories(params.output_dir))
        param_error("Output dir %s must not already exist" format
          params.output_dir)
      // First output the schema file
      val fields = List("corpus", "title", "id", "group", "split", "coord",
        "time", "username", "userid", "reply_username", "reply_userid",
        "anchor", "lang", "text")
      val schema_file_name =
        "%s/twitter-infochimps-schema.txt" format params.output_dir
      val schema_stream = filehand.openw(schema_file_name)
      schema_stream.println(fields mkString "\t")
      schema_stream.close()

      new ConvertTwitterInfochimpsFileProcessor(params).
        process_files(filehand, params.files)
    }
  }
}

object ConvertTwitterInfochimps extends
    ExperimentDriverApp("Convert Twitter Infochimps") {
  type DriverType = ConvertTwitterInfochimpsDriver
  def create_param_object(ap: ArgParser) = new ParamType(ap)
  def create_driver() = new DriverType
}
