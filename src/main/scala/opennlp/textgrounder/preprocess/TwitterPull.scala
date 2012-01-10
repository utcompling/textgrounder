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
  type Tweet = (Long, String, Double, Double, Int, Int)
  type Record = (String, Tweet)
  type Author = (String, Long, Double, Double, Int, Int)
  type AuthorWord = (Author, String)
  type WordCount = (String, Long)

  def force_value(value: json.JValue): String = {
    if ((value values) == null)
        null
    else
        (value values) toString
  }

  def parse_time(timestring: String): Long = {
    val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
    try {
      sdf.parse(timestring)
      sdf.getCalendar.getTimeInMillis
    } catch {
      case pe: ParseException => 0
    }
  }

  val MAX_NUMBER_FOLLOWING = 1000
  val MIN_NUMBER_FOLLOWING = 1
  val MIN_NUMBER_FOLLOWERS = 10
  def is_valid_tweet(r: Record): Boolean = {
    // filters out invalid tweets, as well as trivial spam
    // TODO: filter spam
    val (a, (ts, text, lat, lng, fers, fing)) = r
    ts != 0 && a != "" && 
      (fing >= MIN_NUMBER_FOLLOWING && fing <= MAX_NUMBER_FOLLOWING) &&
      (fers >= MIN_NUMBER_FOLLOWERS)
  }

  val empty_tweet: Record = ("", (0, "", Double.NaN, Double.NaN, 0, 0))

  def parse_json(line: String): Record = {
    try {
      val parsed = json.parse(line)
      val author = force_value(parsed \ "user" \ "screen_name")
      val timestamp = parse_time(force_value(parsed \ "created_at"))
      val text = force_value(parsed \ "text")
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
      (author, (timestamp, text, lat, lng, followers, following))
    } catch {
      case jpe: json.JsonParser.ParseException => empty_tweet
      case npe: NullPointerException => empty_tweet
      case nfe: NumberFormatException => empty_tweet
    }
  }

  def merge_records(tweet1: Tweet, tweet2: Tweet): Tweet = {
    val (ts1, text1, lat1, lng1, fers1, fing1) = tweet1
    val (ts2, text2, lat2, lng2, fers2, fing2) = tweet2
    val (fers, fing) = (math.max(fers1, fers2), math.max(fing1, fing2))
    val text = text1 + " " + text2

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

    (ts, text, lat, lng, fers, fing)
  }

  def has_latlng(r: Record): Boolean = {
    val (a, (ts, text, lat, lng, fers, fing)) = r
    !isNaN(lat) && !isNaN(lng)
  }

  def normalize_word(word: String): String = {
    val lower = word.toLowerCase
    if (lower.startsWith("#"))
      lower.substring(1)
    else
      lower
  }

  def tokenize(text: String): Iterable[String] = {
    return Twokenize(text).map(normalize_word)
  }

  def filter_word(word: String): Boolean = {
    word.contains("http://") ||
      word.contains(":") ||
      word.startsWith("@")
  }

  def emit_words(r: Record): Iterable[(AuthorWord, Long)] = {
    val (author, (ts, text, lat, lng, fers, fing)) = r
    for (word <- tokenize(text) if !filter_word(word))
      yield (((author, ts, lat, lng, fers, fing), word), 1L)
  }

  def reposition_word(awc: (AuthorWord, Long)): (Author, WordCount) = {
    val ((author, word), c) = awc
    (author, (word, c))
  }

  def condense_wordcounts(awcs: (Author, Iterable[WordCount])): (Author, String) = {
    //val ((author, ts, lat, lng, fers, fing), wcs) = awcs
    val (author, wcs) = awcs
    val nice_text = wcs.map((w: WordCount) => w._1 + ":" + w._2).mkString(" ")
    //author + "\t" + ts + "\t" + lat + "," + lng + "\t" + fers + "\t" + fing + "\t" + nice_text
    (author, nice_text)
  }

  def score_user(awcs_s: (Author, String)): (Double, (Author, String)) = {
    val (author, wcs_s) = awcs_s
    val (a, ts, lat, lng, fers, fing): Author = author
    // we actually want to *reverse* sort by fers / fing, but we can't
    // reverse sort, so we'll take the reciprocal to reverse.
    val score = fing.toDouble / fers
    (score, awcs_s)
  }

  def nicely_format(scored_awcs_ses: (Double, Iterable[(Author, String)])): Iterable[String] = {
    val (score, awcs_ses) = scored_awcs_ses
    for (awcs_s <- awcs_ses;
          val (author, nice_text) = awcs_s;
          val (a, ts, lat, lng, fers, fing) = author)
        yield (a + "\t" + ts + "\t" + lat + "," + lng + "\t" + fers + "\t" + fing + "\t" + nice_text)
  }

  def main(args: Array[String]) = withHadoopArgs(args) { a =>

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
    val values_extracted = lines.map(parse_json)
                                .filter(is_valid_tweet)

    // group by author, combine the records, keeping the earliest
    // tweet with a specific coordinate
    val concatted = values_extracted.groupByKey.combine(merge_records)

    // filter every user that doesn't have a specific coordinate
    val with_coord = concatted.filter(has_latlng)

    // word count
    val emitted_words = with_coord.flatMap(emit_words)
    val word_counts = emitted_words.groupByKey.combine((a: Long, b: Long) => (a + b))

    // regroup with author as key, word pairs as values
    val regrouped_by_author = word_counts.map(reposition_word).groupByKey

    // sort by the follower/following ratio
    val sorted_by_ratio = regrouped_by_author.map(condense_wordcounts)
                                             .map(score_user).groupByKey

    //val nicely_formatted = word_counts.map(record_to_string)
    val nicely_formatted = sorted_by_ratio.flatMap(nicely_format)

    // save to disk
    DList.persist(TextOutput.toTextFile(nicely_formatted, outputPath))

  }
}

