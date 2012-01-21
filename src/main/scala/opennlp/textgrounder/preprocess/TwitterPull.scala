package opennlp.textgrounder.preprocess

import net.liftweb.json
import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import java.io._
import java.lang.Double.isNaN
import java.text.{SimpleDateFormat, ParseException}

import opennlp.textgrounder.util.Twokenize

/*
 * This program takes, as input, files which contain one tweet
 * per line in json format as directly pulled from the twitter
 * api. It outputs a folder that may be used as the
 * --input-corpus argument of tg-geolocate.
 */

object TwitterPull {
  // tweet = (Timestamp, Text, Lat, Lng, Followers, Following, Number of tweets (since we merge tweets)
  type Tweet = (Int, String, Double, Double, Int, Int, Int)
  // record = (username, Tweet, # of tweets represented)
  type Record = (String, Tweet)
  // author = (username, earliest timestamp, best lat, best lng, max followers, max following, number of tweets pulled)
  type Author = (String, Int, Double, Double, Int, Int, Int)
  type AuthorWord = (String, String)
  // wordcount = (word, number of ocurrences)
  type WordCount = (String, Long)

  def force_value(value: json.JValue): String = {
    if ((value values) == null)
        null
    else
        (value values) toString
  }

  def parse_time(timestring: String): Int = {
    val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
    try {
      sdf.parse(timestring)
      (sdf.getCalendar.getTimeInMillis / 1000).toInt
    } catch {
      case pe: ParseException => 0
    }
  }

  def is_valid_tweet(id_r: (String, Record)): Boolean = {
    // filters out invalid tweets, as well as trivial spam
    val (tw_id, (a, (ts, text, lat, lng, fers, fing, numtw))) = id_r
    tw_id != "" && ts != 0 && a != "" && !(lat == 0.0 && lng == 0.0)
  }

  val MAX_NUMBER_FOLLOWING = 1000
  val MIN_NUMBER_FOLLOWING = 5
  val MIN_NUMBER_FOLLOWERS = 10
  val MIN_NUMBER_TWEETS = 10
  val MAX_NUMBER_TWEETS = 1000
  def is_nonspammer(r: Record): Boolean = {
    val (a, (ts, text, lat, lng, fers, fing, numtw)) = r

    (fing >= MIN_NUMBER_FOLLOWING && fing <= MAX_NUMBER_FOLLOWING) &&
      (fers >= MIN_NUMBER_FOLLOWERS) &&
      (numtw >= MIN_NUMBER_TWEETS && numtw <= MAX_NUMBER_TWEETS)
  }

  // bounding box for north america
  val MIN_LAT = 25.0
  val MIN_LNG = -126.0
  val MAX_LAT = 49.0
  val MAX_LNG = -60.0
  def northamerica_only(r: Record): Boolean = {
    val (a, (ts, text, lat, lng, fers, fing, numtw)) = r

    (lat >= MIN_LAT && lat <= MAX_LAT) &&
      (lng >= MIN_LNG && lng <= MAX_LNG)
  }


  val empty_tweet: (String, Record) = ("", ("", (0, "", Double.NaN, Double.NaN, 0, 0, 0)))

  def parse_json(line: String): (String, Record) = {
    try {
      val parsed = json.parse(line)
      val author = force_value(parsed \ "user" \ "screen_name")
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
      (tweet_id, (author, (timestamp, text, lat, lng, followers, following, 1)))
    } catch {
      case jpe: json.JsonParser.ParseException => empty_tweet
      case npe: NullPointerException => empty_tweet
      case nfe: NumberFormatException => empty_tweet
    }
  }

  def tweet_once(id_rs: (String, Iterable[Record])): Record = {
    val (id, rs) = id_rs
    rs.head
  }

  def merge_records(tweet1: Tweet, tweet2: Tweet): Tweet = {
    val (ts1, text1, lat1, lng1, fers1, fing1, numtw1) = tweet1
    val (ts2, text2, lat2, lng2, fers2, fing2, numtw2) = tweet2
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

    (ts, text, lat, lng, fers, fing, numtw)
  }

  def has_latlng(r: Record): Boolean = {
    val (a, (ts, text, lat, lng, fers, fing, numtw)) = r
    !isNaN(lat) && !isNaN(lng)
  }

  def normalize_word(word: String): String = {
    word.toLowerCase
  }

  def tokenize(text: String): Iterable[String] = {
    return Twokenize(text).map(normalize_word)
  }

  def filter_word(word: String): Boolean = {
    word.contains("http://") ||
      word.contains(":") ||
      word.startsWith("@")
  }

  def checkpoint_str(r: Record): String = {
    val (author, (ts, text, lat, lng, fers, fing, numtw)) = r
    val text_2 = text.replaceAll("\\s+", " ")
    val s = Seq(author, ts, lat, lng, fers, fing, numtw, "", text_2) mkString "\t"
    s
  }

  def from_checkpoint_to_record(line: String): Record = {
    val split = line.split("\t\t")
    val (author_data, text) = (split(0), split(1))
    val split2 = author_data.split("\t")
    val author = split2(0)
    val ts = split2(1).toInt
    val lat = split2(2).toDouble
    val lng = split2(3).toDouble
    val fers = split2(4).toInt
    val fing = split2(5).toInt
    val numtw = split2(6).toInt
    (author, (ts, text, lat, lng, fers, fing, numtw))
  }

  def from_checkpoint_to_author_text(line: String): (String, String) = {
    val split = line.split("\t\t")
    if (split.length != 2) {
      System.err.println("Bad line: " + line)
      ("", "")
    } else {
      (split(0), split(1))
    }
  }

  def emit_words(author_text: (String, String)): Iterable[(AuthorWord, Long)] = {
    val (authordata, text) = author_text
    for (word <- tokenize(text) if !filter_word(word))
      yield ((authordata, word), 1L)
  }

  def reposition_word(awc: (AuthorWord, Long)): (String, WordCount) = {
    val ((author, word), c) = awc
    (author, (word, c))
  }

  def nicely_format_plain(awcs: (String, Iterable[WordCount])): String = {
    val (author, wcs) = awcs
    val nice_text = wcs.map((w: WordCount) => w._1 + ":" + w._2).mkString(" ")
    author + "\t" + nice_text
  }

  def main(args: Array[String]) = withHadoopArgs(args) { a =>

    /*
    // make sure we get all the input
    val (inputPath, outputPath) =
      if (a.length == 2) {
        (a(0), a(1))
      } else {
        sys.error("Expecting input and output path.")
      }

    // Firstly we load up all the (new-line-seperated) json lines
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    // Filter out some trivially invalid tweets and parse the json
    val values_extracted = lines.map(parse_json).filter(is_valid_tweet)

    val single_tweets = values_extracted.groupByKey.map(tweet_once)

    val checkpoint = single_tweets.map(checkpoint_str)
    DList.persist(TextOutput.toTextFile(checkpoint, outputPath + "-st"))

    val lines: DList[String] = TextInput.fromTextFile(outputPath + "-st")
    val values_extracted = lines.map(from_checkpoint_to_record)

    // group by author, combine the records, keeping the earliest
    // tweet with a specific coordinate
    val concatted = values_extracted.groupByKey.combine(merge_records)

    // filter every user that doesn't have a specific coordinate
    val with_coord = concatted.filter(has_latlng)
                              .filter(is_nonspammer)
                              .filter(northamerica_only)


    val checkpoint = with_coord.map(checkpoint_str)
    DList.persist(TextOutput.toTextFile(checkpoint, outputPath + "-cp"))

    // load from the checkpoint
    val lines_cp: DList[String] = TextInput.fromTextFile(outputPath + "-cp")
    // word count
    val emitted_words = lines_cp.map(from_checkpoint_to_author_text)
                                .flatMap(emit_words)
    val word_counts = emitted_words.groupByKey.combine((a: Long, b: Long) => (a + b))

    // regroup with author as key, word pairs as values
    val regrouped_by_author = word_counts.map(reposition_word).groupByKey

    // nice string output
    val nicely_formatted = regrouped_by_author.map(nicely_format_plain)

    // save to disk
    DList.persist(TextOutput.toTextFile(nicely_formatted, outputPath))
    */
  }
}

