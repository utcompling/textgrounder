///////////////////////////////////////////////////////////////////////////////
//  ProcessTwitterPull.scala
//
//  Copyright (C) 2012 Stephen Roller, The University of Texas at Austin
//  Copyright (C) 2012 Ben Wing, The University of Texas at Austin
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

import net.liftweb.json
import com.nicta.scoobi.Scoobi._
import java.io._
import java.lang.Double.isNaN
import java.text.{SimpleDateFormat, ParseException}

import util.control.Breaks._

import opennlp.textgrounder.util.Twokenize

/*
 * This program takes, as input, files which contain one tweet
 * per line in json format as directly pulled from the twitter
 * API. It combines the tweets either by user or by time, and outputs a
 * folder that may be used as the --input-corpus argument of tg-geolocate.
 * This is in "TextGrounder corpus" format, with one document per line,
 * fields separated by tabs, and all the words and counts placed in a single
 * field, of the form "WORD1:COUNT1 WORD2:COUNT2 ...".
 *
 * The fields currently output are:
 *
 * 1. user
 * 2. timestamp
 * 3. latitude,longitude
 * 4. number of followers (people following the user)
 * 5. number of people the user is following
 * 6. number of tweets merged to form the per-user document
 * 7. unigram text of combined tweets
 *
 * NOTE: A schema is generated and properly named, but the main file is
 * currently given a name by Scoobi and needs to be renamed to correspond
 * with the schema file: e.g. if the schema file is called
 * "sep-22-debate-training-unigram-counts-schema.txt", then the main file
 * should be called "sep-22-debate-training-unigram-counts.txt".
 *
 * When merging by user, the code uses the earliest geolocated tweet as the
 * user's location; tweets with a bounding box as their location rather than a
 * single point are treated as if they have no location.  Then the code filters
 * out users that have no geolocation or have a geolocation outside of North
 * America (according to a crude bounding box), as well as those that are
 * identified as likely "spammers" according to their number of followers and
 * number of people they are following.
 */

class TwitterPullOptions {
  var keytype = "user"
  var timeslice = 6000L
  var corpus_name = "unknown"
  var split = "training"
}

object TwitterPull extends ScoobiApp {
  // Tweet = Data for a tweet other than the tweet ID =
  // (user, timestamp, text, lat, lng, followers, following, number of tweets)
  // Note that we have "number of tweets" since we merge multiple tweets into
  // a document, and also use type Tweet for them.
  type Tweet = (String, Long, String, Double, Double, Int, Int, Int)
  // TweetID = numeric string used to uniquely identify a tweet.
  type TweetID = String
  // Record = Data for tweet along with the key (e.g. username, timestamp) =
  // (key, tweet data)
  type Record = (String, Tweet)
  // IDRecord = Tweet ID along with all other data for a tweet.
  type IDRecord = (TweetID, Record)
  // TweetNoText = Data for a merged set of tweets other than the text =
  // (username, earliest timestamp, best lat, best lng, max followers,
  //    max following, number of tweets pulled)
  type TweetNoText = (String, Long, Double, Double, Int, Int, Int)
  // TweetWord = Data for the tweet minus the text, plus an individual word
  //   from the text = (tweet_no_text_as_string, word)
  type TweetWord = (String, String)
  // WordCount = (word, number of ocurrences)
  type WordCount = (String, Long)

  def force_value(value: json.JValue): String = {
    if ((value values) == null)
        null
    else
        (value values) toString
  }

  /**
   * Convert a Twitter timestamp, e.g. "Tue Jun 05 14:31:21 +0000 2012", into
   * a time in milliseconds since the Epoch (Jan 1 1970, or so).
   */
  def parse_time(timestring: String): Long = {
    val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
    try {
      sdf.parse(timestring)
      sdf.getCalendar.getTimeInMillis
    } catch {
      case pe: ParseException => 0
    }
  }

  /**
   * Return true if this tweet is "valid" in that it doesn't have any
   * out-of-range values (blank strings or 0-valued quantities).  Note
   * that we treat a case where both latitude and longitude are 0 as
   * invalid even though technically such a place could exist. (FIXME,
   * use NaN or something to indicate a missing latitude or longitude).
   */
  def is_valid_tweet(id_r: IDRecord): Boolean = {
    // filters out invalid tweets, as well as trivial spam
    val (tw_id, (key, (user, ts, text, lat, lng, fers, fing, numtw))) = id_r
    tw_id != "" && ts != 0.0 && user != "" && !(lat == 0.0 && lng == 0.0)
  }

  val MAX_NUMBER_FOLLOWING = 1000
  val MIN_NUMBER_FOLLOWING = 5
  val MIN_NUMBER_FOLLOWERS = 10
  val MIN_NUMBER_TWEETS = 10
  val MAX_NUMBER_TWEETS = 1000
  /**
   * Return true if this tweet combination (tweets for a given user)
   * appears to reflect a "spammer" user or some other user with
   * sufficiently nonstandard behavior that we want to exclude them (e.g.
   * a celebrity or an inactive user): Having too few or too many tweets,
   * following too many or too few, or having too few followers.  A spam
   * account is likely to have too many tweets -- and even more, to send
   * tweets to too many people (although we don't track this).  A spam
   * account sends much more than it receives, and may have no followers.
   * A celebrity account receives much more than it sends, and tends to have
   * a lot of followers.  People who send too few tweets simply don't
   * provide enough data.
   *
   * FIXME: We don't check for too many followers of a given account, but
   * instead too many people that a given account is following.  Perhaps
   * this is backwards?
   */
  def is_nonspammer(r: Record): Boolean = {
    val (key, (user, ts, text, lat, lng, fers, fing, numtw)) = r

    (fing >= MIN_NUMBER_FOLLOWING && fing <= MAX_NUMBER_FOLLOWING) &&
      (fers >= MIN_NUMBER_FOLLOWERS) &&
      (numtw >= MIN_NUMBER_TWEETS && numtw <= MAX_NUMBER_TWEETS)
  }

  // bounding box for north america
  val MIN_LAT = 25.0
  val MIN_LNG = -126.0
  val MAX_LAT = 49.0
  val MAX_LNG = -60.0
  /**
   * Return true of this tweet (combination) is located within the
   * bounding box fo North America.
   */
  def northamerica_only(r: Record): Boolean = {
    val (key, (user, ts, text, lat, lng, fers, fing, numtw)) = r

    (lat >= MIN_LAT && lat <= MAX_LAT) &&
      (lng >= MIN_LNG && lng <= MAX_LNG)
  }


  /**
   * An empty tweet, stored as a full IDRecord.
   */
  val empty_tweet: IDRecord = ("", ("", ("", 0, "", Double.NaN, Double.NaN, 0, 0, 0)))

  /**
   * Parse a JSON line into a tweet.  Return value is an IDRecord, including
   * the tweet ID, username, text and all other data.
   */
  def parse_json(line: String, opts: TwitterPullOptions): IDRecord = {
    try {
      val parsed = json.parse(line)
      val user = force_value(parsed \ "user" \ "screen_name")
      val timestamp = parse_time(force_value(parsed \ "created_at"))
      val text = force_value(parsed \ "text").replaceAll("\\s+", " ")
      val followers = force_value(parsed \ "user" \ "followers_count").toInt
      val following = force_value(parsed \ "user" \ "friends_count").toInt
      val tweet_id = force_value(parsed \ "id_str")
      val (lat, lng) = 
        if ((parsed \ "coordinates" values) == null ||
            (force_value(parsed \ "coordinates" \ "type") != "Point")) {
          (Double.NaN, Double.NaN)
        } else {
          val latlng: List[Number] = 
            (parsed \ "coordinates" \ "coordinates" values).asInstanceOf[List[Number]]
          (latlng(1).doubleValue, latlng(0).doubleValue)
        }
      val key = opts.keytype match {
        case "user" => user
        case _ => ((timestamp / opts.timeslice) * opts.timeslice).toString
      }
      (tweet_id, (key, (user, timestamp, text, lat, lng, followers, following, 1)))
    } catch {
      case jpe: json.JsonParser.ParseException => empty_tweet
      case npe: NullPointerException => empty_tweet
      case nfe: NumberFormatException => empty_tweet
    }
  }

  // Select the first tweet with the same ID.  For various reasons we may
  // have duplicates of the same tweet among our data.  E.g. it seems that
  // Twitter itself sometimes streams duplicates through its Streaming API,
  // and data from different sources will almost certainly have duplicates.
  // Furthermore, sometimes we want to get all the tweets even in the presence
  // of flakiness that causes Twitter to sometimes bomb out in a Streaming
  // session and take a while to restart, so we have two or three simultaneous
  // streams going recording the same stuff, hoping that Twitter bombs out at
  // different points in the different sessions (which is generally true).
  // Then, all or almost all the tweets are available in the different streams,
  // but there is a lot of duplication that needs to be tossed aside.
  def tweet_once(id_rs: (TweetID, Iterable[Record])): Record = {
    val (id, rs) = id_rs
    rs.head
  }

  /**
   * Merge the data associated with two tweets or tweet combinations
   * into a single tweet combination.  Concatenate text.  Find maximum
   * numbers of followers and followees.  Add number of tweets in each.
   * For latitude and longitude, take the earliest provided values
   * ("earliest" by timestamp and "provided" meaning not missing).
   */
  def merge_records(tweet1: Tweet, tweet2: Tweet): Tweet = {
    val (user1, ts1, text1, lat1, lng1, fers1, fing1, numtw1) = tweet1
    val (user2, ts2, text2, lat2, lng2, fers2, fing2, numtw2) = tweet2
    val (fers, fing) = (math.max(fers1, fers2), math.max(fing1, fing2))
    val text = text1 + " " + text2
    val numtw = numtw1 + numtw2

    val (lat, lng, ts) = 
      if (isNaN(lat1) && isNaN(lat2)) {
        (lat1, lng1, math.min(ts1, ts2))
      } else if (isNaN(lat2)) {
        (lat1, lng1, ts1)
      } else if (isNaN(lat1)) {
        (lat2, lng2, ts2)
      } else if (ts1 < ts2) {
        (lat1, lng1, ts1)
      } else {
        (lat2, lng2, ts2)
      }

    // FIXME maybe want to track the different users
    (user1, ts, text, lat, lng, fers, fing, numtw)
  }

  /**
   * Return true if tweet (combination) has a fully-specified latitude
   * and longitude.
   */
  def has_latlng(r: Record): Boolean = {
    val (key, (user, ts, text, lat, lng, fers, fing, numtw)) = r
    !isNaN(lat) && !isNaN(lng)
  }

  /**
   * Convert a word to lowercase.
   */
  def normalize_word(word: String): String = {
    word.toLowerCase
  }

  /**
   * Use Twokenize to break up a tweet into separate tokens.
   */
  def tokenize(text: String): Iterable[String] = {
    return Twokenize(text).map(normalize_word)
  }

  def filter_word(word: String): Boolean = {
    word.contains("http://") ||
      word.contains(":") ||
      word.startsWith("@")
  }

  def checkpoint_str(r: Record): String = {
    val (key, (user, ts, text, lat, lng, fers, fing, numtw)) = r
    val text_2 = text.replaceAll("\\s+", " ")
    val s = Seq(key, user, ts, lat, lng, fers, fing, numtw, "", text_2) mkString "\t"
    s
  }

  def from_checkpoint_to_record(line: String): Record = {
    val split = line.split("\t\t")
    val (tweet_no_text, text) = (split(0), split(1))
    val (key, (user, ts, lat, lng, fers, fing, numtw)) =
      split_tweet_no_text(tweet_no_text)
    (key, (user, ts, text, lat, lng, fers, fing, numtw))
  }

  def split_tweet_no_text(tweet_no_text: String): (String, TweetNoText) = {
    val split2 = tweet_no_text.split("\t")
    val key = split2(0)
    val user = split2(1)
    val ts = split2(2).toLong
    val lat = split2(3).toDouble
    val lng = split2(4).toDouble
    val fers = split2(5).toInt
    val fing = split2(6).toInt
    val numtw = split2(7).toInt
    (key, (user, ts, lat, lng, fers, fing, numtw))
  }

  def from_checkpoint_to_tweet_text(line: String): (String, String) = {
    val split = line.split("\t\t")
    if (split.length != 2) {
      System.err.println("Bad line: " + line)
      ("", "")
    } else {
      (split(0), split(1))
    }
  }

  /**
   * Given the tweet data minus the text field combined into a string,
   * plus the text field, tokenize the text and emit the words individually.
   * Each word is emitted along with the text data and a count of 1,
   * and later grouping + combining will add all the 1's to get the
   * word count.
   */
  def emit_words(tweet_text: (String, String)): Iterable[(TweetWord, Long)] = {
    val (tweet_no_text, text) = tweet_text
    for (word <- tokenize(text) if !filter_word(word))
      yield ((tweet_no_text, word), 1L)
  }

  /**
   * We made the value in the key-value pair be a count so we can combine
   * the counts easily to get the total word count, and stuffed all the
   * rest of the data (tweet data plus word) into the key, but now we have to
   * rearrange to move the word back into the value.
   */
  def reposition_word(twc: (TweetWord, Long)): (String, WordCount) = {
    val ((tweet_no_text, word), c) = twc
    (tweet_no_text, (word, c))
  }

  /**
   * Given tweet data minus text plus an iterable of word-count pairs,
   * convert to a string suitable for outputting.
   */
  def nicely_format_plain(twcs: (String, Iterable[WordCount])): String = {
    val (tweet_no_text, wcs) = twcs
    val nice_text = wcs.map((w: WordCount) => w._1 + ":" + w._2).mkString(" ")
    val (key, (user, ts, lat, lng, fers, fing, numtw)) =
      split_tweet_no_text(tweet_no_text)
    // Latitude/longitude need to be combined into a single field, but only
    // if both actually exist.
    val latlngstr =
      if (!isNaN(lat) && !isNaN(lng))
        "%s,%s" format (lat, lng)
      else ""
    // Put back together but drop key.
    Seq(user, ts, latlngstr, fers, fing, numtw, nice_text) mkString "\t"
  }

  def output_schema(outputPath: String, opts: TwitterPullOptions) {
    val filename =
      "%s/%s-%s-unigram-counts-schema.txt" format
        (outputPath, opts.corpus_name, opts.split)
    val p = new PrintWriter(new File(filename))
    def print_seq(s: String*) {
      p.println(s mkString "\t")
    }
    try {
      print_seq("user", "timestamp", "coord", "followers", "following",
        "numtweets", "counts")
      print_seq("corpus", opts.corpus_name)
      print_seq("corpus-type", "twitter-%s" format opts.keytype)
      if (opts.keytype == "timestamp")
        print_seq("corpus-timeslice", opts.timeslice.toString)
      print_seq("split", opts.split)
    } finally { p.close() }
  }

  def run() {
    val opts = new TwitterPullOptions
    var a = args

    breakable {
      while (a.length > 0) {
        if (a(0) == "--by-time") {
          opts.keytype = "timestamp"
          a = a.tail
        } else if (a(0) == "--timeslice" || a(0) == "--time-slice") {
          opts.timeslice = (a(1).toDouble * 1000).toLong
          a = a.tail.tail
        } else if (a(0) == "--name") {
          opts.corpus_name = a(1)
          a = a.tail.tail
        } else if (a(0) == "--split") {
          opts.split = a(1)
          a = a.tail.tail
        } else break
      }
    }

    // make sure we get all the input
    val (inputPath, outputPath) =
      if (a.length == 2) {
        (a(0), a(1))
      } else {
        sys.error("Expecting input and output path.")
      }

    // Firstly we load up all the (new-line-separated) JSON lines.
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    // Parse JSON into tweet records (IDRecord), filter out invalid tweets.
    val values_extracted = lines.map(parse_json(_, opts)).filter(is_valid_tweet)

    // Filter out duplicate tweets -- group by Tweet ID and then take the
    // first tweet for a given ID.  Duplicate tweets occur for various
    // reasons, e.g. sometimes in the stream itself due to Twitter errors,
    // or when combining data from multiple, possibly overlapping, sources.
    // In the process, the tweet ID's are discarded.
    val single_tweets = values_extracted.groupByKey.map(tweet_once)

    // Checkpoint the resulting tweets (minus ID) onto disk.
    val checkpoint1 = single_tweets.map(checkpoint_str)
    persist(TextOutput.toTextFile(checkpoint1, outputPath + "-st"))

    // Then load back up.
    val lines2: DList[String] = TextInput.fromTextFile(outputPath + "-st")
    val values_extracted2 = lines2.map(from_checkpoint_to_record)

    // Group by username, then combine the tweets for a user into a
    // tweet combination, with text concatenated and the location taken
    // from the earliest/ tweet with a specific coordinate.
    val concatted = values_extracted2.groupByKey.combine(merge_records)

    // If grouping by user, filter the tweet combinations, removing users
    // without a specific coordinate; users that appear to be "spammers" or
    // other users with non-standard behavior; and users located outside of
    // North America.  FIXME: We still want to filter spammers; but this
    // is trickier when not grouping by user.  How to do it?
    val with_coord =
      if (opts.keytype == "timestamp") concatted
      else
        concatted.filter(has_latlng).filter(is_nonspammer).
          filter(northamerica_only)


    // Checkpoint a second time.
    val checkpoint = with_coord.map(checkpoint_str)
    persist(TextOutput.toTextFile(checkpoint, outputPath + "-cp"))

    // Load from second checkpoint.  Note that each time we checkpoint,
    // we extract the "text" field and stick it at the end.  This time
    // when loading it up, we use `from_checkpoint_to_tweet_text`, which
    // gives us the tweet data as a string (minus text) and the text, as
    // two separate strings.
    val lines_cp: DList[String] = TextInput.fromTextFile(outputPath + "-cp")

    // Now word count.  We run `from_checkpoint_to_tweet_text` (see above)
    // to get separate access to the text, then Twokenize it into words
    // and emit a series of key-value pairs of (word, count).
    val emitted_words = lines_cp.map(from_checkpoint_to_tweet_text)
                                .flatMap(emit_words)
    // Group based on key (the word) and combine by adding up the individual
    // counts.
    val word_counts = emitted_words.groupByKey.combine((a: Long, b: Long) => (a + b))

    // Regroup with proper key (user, timestamp, etc.) as key,
    // word pairs as values.
    val regrouped_by_key = word_counts.map(reposition_word).groupByKey

    // Nice string output.
    val nicely_formatted = regrouped_by_key.map(nicely_format_plain)

    // Save to disk.
    persist(TextOutput.toTextFile(nicely_formatted, outputPath))

    // create a schema
    output_schema(outputPath, opts)
  }
}

