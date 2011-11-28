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

import java.io.PrintStream

import org.apache.commons.lang3.StringEscapeUtils._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil.{FileHandler, LocalFileHandler, TextFileProcessor}
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.osutil.output_resource_usage
import opennlp.textgrounder.util.printutil._
import opennlp.textgrounder.util.textutil.long_with_commas
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
    ProcessFilesParameters(ap) {
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
          //val raw_anchor = unescapeXml(anchor)
          // Go ahead and leave it encoded, just to make sure no TAB chars;
          // we don't really use it much anyway.
          val raw_anchor = anchor
          assert(!(raw_anchor contains '\t'))
          val metadata =
            Seq("corpus"->"twitter-infochimps",
                "id"->id, "title"->id, "split"->"training",
                "coord"->("%s,%s" format (lat, long)),"time"->time,
                "username"->username, "userid"->userid,
                "reply_username"->reply_username, "reply_userid"->reply_userid,
                "anchor"->raw_anchor, "lang"->lang)
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
    output_resource_usage()
    true
  }

  // To be implemented

  def process_line(metadata: Seq[(String, String)], text: String)
}

class ConvertTwitterInfochimpsFileProcessor(
  params: ConvertTwitterInfochimpsParameters,
  suffix: String
) extends TwitterInfochimpsFileProcessor {
  var outstream: PrintStream = _
  val compression_type = "bzip2"
  var schema: Seq[String] = null
  var current_filehand: FileHandler = _

  override def begin_process_lines(lines: Iterator[String],
      filehand: FileHandler, file: String,
      compression: String, realname: String) {
    val (_, outname) = filehand.split_filename(realname)
    val out_text_name = "%s/twitter-infochimps-%s%s.txt" format (
      params.output_dir, outname, suffix)
    errprint("Text document file is %s..." format out_text_name)
    current_filehand = filehand
    outstream = filehand.openw(out_text_name, compression = compression_type)
    super.begin_process_lines(lines, filehand, file, compression, realname)
  }

  def process_line(metadata: Seq[(String, String)], text: String) {
    val rawtext = unescapeHtml4(unescapeXml(text))
    val splittext = Twokenize(rawtext)
    val outdata = metadata ++ Seq("text"->(splittext mkString " "))
    val (schema, fieldvals) = outdata.unzip
    if (this.schema == null) {
      // Output the schema file, first time we see a line
      val schema_file_name =
        "%s/twitter-infochimps%s-schema.txt" format (params.output_dir, suffix)
      val schema_stream = current_filehand.openw(schema_file_name)
      errprint("Schema file is %s..." format schema_file_name)
      schema_stream.println(schema mkString "\t")
      schema_stream.close()
      this.schema = schema
    } else
      assert (this.schema == schema)
    outstream.println(fieldvals mkString "\t")
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    outstream.close()
    super.end_process_file(filehand, file)
  }
}

class TwitterStatistics {
  // Over-all statistics
  var num_tweets = 0

  val max_string_val = "\uFFFF"
  val min_string_val = ""
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
  var earliest_tweet_time = max_string_val
  var latest_tweet_time = min_string_val
  var earliest_tweet_id = max_string_val
  var latest_tweet_id = min_string_val

  // If tweet ID's are sorted by time, the following two should be
  // identical to the earliest_tweet_id/latest_tweet_id.  Tweets ID's
  // may have exceeeded Long range; if not, they probably will soon.
  var num_lowest_tweet_id = BigInt("9"*50)
  var num_lowest_tweet_time = ""
  var num_highest_tweet_id = BigInt(-1)
  var num_highest_tweet_time = ""

  // Similar but we sort tweet ID's lexicographically.
  var lex_lowest_tweet_id = max_string_val
  var lex_lowest_tweet_time = ""
  var lex_highest_tweet_id = ""
  var lex_highest_tweet_time = ""

  // User-based statistics
  val tweets_by_user = intmap[String]()
  val reply_tweets_by_user = intmap[String]()
  val num_users_by_num_tweets = intmap[Int]()
  val userid_by_user = intmapmap[String, String]()
  val userid_by_user_min_time = stringmapmap[String, String](max_string_val)
  val userid_by_user_max_time = stringmapmap[String, String](min_string_val)
  val user_by_userid = intmapmap[String, String]()
  val user_by_userid_min_time = stringmapmap[String, String](max_string_val)
  val user_by_userid_max_time = stringmapmap[String, String](min_string_val)
  val tweets_by_reply_user = intmap[String]()
  val reply_userid_by_reply_user = intmapmap[String, String]()
  val reply_userid_by_reply_user_min_time = stringmapmap[String, String](max_string_val)
  val reply_userid_by_reply_user_max_time = stringmapmap[String, String](min_string_val)
  val reply_user_by_reply_userid = intmapmap[String, String]()
  val reply_user_by_reply_userid_min_time = stringmapmap[String, String](max_string_val)
  val reply_user_by_reply_userid_max_time = stringmapmap[String, String](min_string_val)
  val reply_user_by_user = intmapmap[String, String]()
  val user_by_reply_user = intmapmap[String, String]()

  def record_tweet(metadata: Seq[(String, String)], text: String) {
    val params = metadata.toMap
    val time = params("time")
    val id = params("id")
    val username = params("username")
    val userid = params("userid")
    val reply_username = params("reply_username")
    val reply_userid = params("reply_userid")

    num_tweets += 1

    // Time format is YYYYMMDDHHmmSS, hence take(8) goes up through the day,
    // take(10) up through the hour, etc.
    tweets_by_day(time.take(8)) += 1
    tweets_by_hour(time.take(10)) += 1
    // tweets_by_minute(time.take(12)) += 1

    if (time < earliest_tweet_time) {
      earliest_tweet_time = time
      earliest_tweet_id = id
    }
    if (time > latest_tweet_time) {
      latest_tweet_time = time
      latest_tweet_id = id
    }
    if (id < lex_lowest_tweet_id) {
      lex_lowest_tweet_id = id
      lex_lowest_tweet_time = time
    }
    if (id > lex_highest_tweet_id) {
      lex_highest_tweet_id = id
      lex_highest_tweet_time = time
    }
    val num_id = BigInt(id)
    if (num_id < num_lowest_tweet_id) {
      num_lowest_tweet_id = num_id
      num_lowest_tweet_time = time
    }
    if (num_id > num_highest_tweet_id) {
      num_highest_tweet_id = num_id
      num_highest_tweet_time = time
    }


    def set_max_with_cur[T,U <% Ordered[U]](table: mutable.Map[T,U],
        key: T, newval: U) {
      if (table(key) < newval)
        table(key) = newval
    }
    def set_min_with_cur[T,U <% Ordered[U]](table: mutable.Map[T,U],
        key: T, newval: U) {
      if (table(key) > newval)
        table(key) = newval
    }
    tweets_by_user(username) += 1
    userid_by_user(username)(userid) += 1
    set_max_with_cur(userid_by_user_max_time(username), userid, time)
    set_min_with_cur(userid_by_user_min_time(username), userid, time)
    user_by_userid(userid)(username) += 1
    set_max_with_cur(user_by_userid_max_time(userid), username, time)
    set_min_with_cur(user_by_userid_min_time(userid), username, time)
    if (reply_username != "") {
      reply_tweets_by_user(username) += 1
      tweets_by_reply_user(reply_username) += 1
      reply_userid_by_reply_user(reply_username)(reply_userid) += 1
      reply_user_by_reply_userid(reply_userid)(reply_username) += 1
      set_max_with_cur(reply_userid_by_reply_user_max_time(reply_username), reply_userid, time)
      set_min_with_cur(reply_userid_by_reply_user_min_time(reply_username), reply_userid, time)
      set_max_with_cur(reply_user_by_reply_userid_max_time(reply_userid), reply_username, time)
      set_min_with_cur(reply_user_by_reply_userid_min_time(reply_userid), reply_username, time)
      reply_user_by_user(username)(reply_username) += 1
      user_by_reply_user(reply_username)(username) += 1
    }
  }

  def finish_statistics() {
    num_users_by_num_tweets.clear()
    for ((user, count) <- tweets_by_user)
      num_users_by_num_tweets(count) += 1
  }

  def print_statistics() {
    finish_statistics()

    val how_many_summary = 10000
    val how_many_summary_str = long_with_commas(how_many_summary)
    val how_many_detail = 100
    val how_many_detail_str = long_with_commas(how_many_detail)

    errprint("")
    errprint("Earliest tweet: %s at %s" format
      (earliest_tweet_id, earliest_tweet_time))
    errprint("Numerically lowest tweet ID: %s at %s" format
      (num_lowest_tweet_id, num_lowest_tweet_time))
    errprint("Lexicographically lowest tweet ID: %s at %s" format
      (lex_lowest_tweet_id, lex_lowest_tweet_time))
    errprint("")
    errprint("Latest tweet: %s at %s" format
      (latest_tweet_id, latest_tweet_time))
    errprint("Numerically highest tweet ID: %s at %s" format
      (num_highest_tweet_id, num_highest_tweet_time))
    errprint("Lexicographically highest tweet ID: %s at %s" format
      (lex_highest_tweet_id, lex_highest_tweet_time))
    errprint("")
    errprint("Number of tweets: %s" format num_tweets)
    errprint("Number of users: %s" format tweets_by_user.size)

    print_msg_heading(
      "Top %s users by number of tweets:" format how_many_summary_str)
    output_reverse_sorted_table(tweets_by_user, maxrows = how_many_summary)

    print_msg_heading(
      "Frequency of frequencies (number of users with given number of tweets):")
    output_key_sorted_table(num_users_by_num_tweets)


    def reply_to_details(sending: String, _from: String, _to: String,
        tweets_by_user_map: mutable.Map[String, Int],
        tweets_by_reply_user_map: mutable.Map[String, Int]) {
      print_msg_heading("Reply-to, for top %s %s users:" format
        (how_many_detail_str, sending))
      for (((user, count), index0) <-
          tweets_by_user_map.toSeq.sortWith(_._2 > _._2).
          slice(0, how_many_detail).
          zipWithIndex) {
        val index = index0 + 1
        errprint("#%d: User %s (%d tweets %s, %d tweets %s):",
          index, user, count, _from, tweets_by_reply_user_map(user), _to)
        errprint("#%d: Corresponding user ID's by tweet count:" format (index))
        output_reverse_sorted_table(userid_by_user(user), indent = "   ")
        errprint("#%d: Users that this user replied to:" format (index))
        output_reverse_sorted_table(reply_user_by_user(user), indent = "   ")
        errprint("#%d: Users that relied to this user:" format (index))
        output_reverse_sorted_table(user_by_reply_user(user), indent = "   ")
      }
    }

    reply_to_details("sending", "from", "to", tweets_by_user,
      tweets_by_reply_user)
    reply_to_details("receiving", "to", "from", tweets_by_reply_user,
      tweets_by_user)

    def output_x_with_multi_y(xdesc: String, ydesc: String,
        x_to_y: mutable.Map[String, mutable.Map[String, Int]],
        x_to_y_min_time: mutable.Map[String, mutable.Map[String, String]],
        x_to_y_max_time: mutable.Map[String, mutable.Map[String, String]]
      ) {
      val x_with_multi_y =
        (for ((x, ys) <- x_to_y; if ys.size > 1)
          yield (x, ys.size))
      for (((x, count), index) <-
           x_with_multi_y.toSeq.sortWith(_._2 > _._2).zipWithIndex) {
        errprint("#%d: %s %s (%d different %s's): (listed by num tweets)",
          index + 1, xdesc, x, count, ydesc)
        for ((y, count) <- x_to_y(x).toSeq.sortWith(_._2 > _._2)) {
          errprint("%s%s = %s (from %s to %s)" format
            ("   ", y, count, x_to_y_min_time(x)(y), x_to_y_max_time(x)(y)))
        }
      }
    }

    print_msg_heading("Users with multiple user ID's:")
    output_x_with_multi_y("user", "ID", userid_by_user,
      userid_by_user_min_time, userid_by_user_max_time)

    print_msg_heading("User ID's with multiple users:")
    output_x_with_multi_y("ID", "user", user_by_userid,
      user_by_userid_min_time, user_by_userid_max_time)

    print_msg_heading("Reply users with multiple reply user ID's:")
    output_x_with_multi_y("reply user", "reply ID", reply_userid_by_reply_user,
      reply_userid_by_reply_user_min_time, reply_userid_by_reply_user_max_time)

    print_msg_heading("Reply ID's with multiple reply users:")
    output_x_with_multi_y("reply ID", "reply user", reply_user_by_reply_userid,
      reply_user_by_reply_userid_min_time, reply_user_by_reply_userid_max_time)

    print_msg_heading("Tweets by day:")
    output_key_sorted_table(tweets_by_day)
    print_msg_heading("Tweets by hour:")
    output_key_sorted_table(tweets_by_hour)
    if (tweets_by_minute.size > 0) {
      print_msg_heading("Tweets by minute:")
      output_key_sorted_table(tweets_by_minute)
    }

    print_msg_heading("Memory/time usage:", blank_lines_before = 3)
    output_resource_usage()
  }
}

class TwitterInfochimpsStatsFileProcessor extends
    TwitterInfochimpsFileProcessor {
  var curfile: String = _
  var curfile_stats: TwitterStatistics = _
  val global_stats = new TwitterStatistics

  override def begin_process_file(filehand: FileHandler, file: String) {
    val (_, outname) = filehand.split_filename(file)
    curfile = outname
    curfile_stats = new TwitterStatistics
    super.begin_process_file(filehand, file)
  }

  def process_line(metadata: Seq[(String, String)], text: String) {
    curfile_stats.record_tweet(metadata, text)
    global_stats.record_tweet(metadata, text)
  }

  def print_curfile_stats() {
    print_msg_heading("Statistics for file %s:" format curfile,
      blank_lines_before = 6)
    curfile_stats.print_statistics()
  }

  def print_global_stats(is_final: Boolean = false) {
    print_msg_heading(
      "Statistics for all files%s:" format (if (is_final) "" else " so far"),
      blank_lines_before = 6)
    global_stats.print_statistics()
  }

  override def end_process_file(filehand: FileHandler, file: String) {
    print_curfile_stats()
    print_global_stats()
    super.end_process_file(filehand, file)
  }

  override def end_processing(filehand: FileHandler, files: Iterable[String]) {
    print_global_stats()
    super.end_processing(filehand, files)
  }
}

class ConvertTwitterInfochimpsDriver extends ProcessFilesDriver {
  type ParamType = ConvertTwitterInfochimpsParameters
  
  def usage() {
    sys.error("""Usage: ConvertTwitterInfochimps [-o OUTDIR | --outfile OUTDIR] [--output-stats] INFILE ...

Convert input files in the Infochimps Twitter corpus into files in the
format expected by TextGrounder.  If --output-stats is given,
output statistics to stderr rather than converting text.  Otherwise,
store results in OUTDIR, which must not exist already.  
""")
  }

  override def handle_parameters() {
    if (!params.output_stats)
      super.handle_parameters()
  }

  override def run_after_setup() {
    if (params.output_stats)
      new TwitterInfochimpsStatsFileProcessor().
        process_files(filehand, params.files)
    else {
      super.run_after_setup()
      new ConvertTwitterInfochimpsFileProcessor(params, "-text").
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
