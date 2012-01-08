package opennlp.textgrounder.preprocess

import net.liftweb.json
import com.nicta.scoobi._
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.io.text._
import java.io._
import java.lang.Double.isNaN

object TwitterPull {
  type Tweet = (Long, String, Double, Double)
  type Record = (String, Tweet)
  type Author = (String, Long, Double, Double)
  type AuthorWord = (Author, String)
  type WordCount = (String, Long)

  def force_value(value: json.JValue) : String = {
    if ((value values) == null)
        null
    else
        (value values) toString
  }

  def parse_time(timestring: String): Long = {
    val sdf = new java.text.SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
    sdf.parse(timestring)
    sdf.getCalendar.getTimeInMillis
  }

  def is_valid_tweet(line: String): Boolean = {
    !line.contains("""{"limit":{"track":""")
  }

  def parse_json(line: String): Record = {
    val parsed = json.parse(line)
    val author = force_value(parsed \ "user" \ "screen_name")
    val timestamp = parse_time(force_value(parsed \ "created_at"))
    val text = force_value(parsed \ "text")
    val (lat, lng) = 
      if ((parsed \ "coordinates" values) == null ||
          (force_value(parsed \ "coordinates" \ "type") != "Point")) {
        (Double.NaN, Double.NaN)
      } else {
        val latlng: List[Number] = 
          (parsed \ "coordinates" \ "coordinates" values).asInstanceOf[List[Number]]
        (latlng(0).doubleValue, latlng(1).doubleValue)
      }
    (author, (timestamp, text, lat, lng))
  }

  def merge_records(tweet1: Tweet, tweet2: Tweet): Tweet = {
    val (ts1, text1, lat1, lng1) = tweet1
    val (ts2, text2, lat2, lng2) = tweet2
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

    (ts, text, lat, lng)
  }

  def has_latlng(r: Record): Boolean = {
    val (a, (ts, text, lat, lng)) = r
    !isNaN(lat) && !isNaN(lng)
  }

  def tokenize(text: String): Iterable[String] = {
    return text.replaceAll("\\s+", " ").split(" ")
  }

  val MAX_WORD_SIZE = 32 * 1024 - 1

  def emit_words(r: Record): Iterable[(AuthorWord, Long)] = {
    val (author, (ts, text, lat, lng)) = r
    for (word <- tokenize(text) if word.length < MAX_WORD_SIZE)
      yield (((author, ts, lat, lng), word), 1L)
  }

  def reposition_word(awc: (AuthorWord, Long)): (Author, WordCount) = {
    val ((author, word), c) = awc
    (author, (word, c))
  }

  /*
  def record_to_string(awcs: (Author, Iterable[WordCount])): String = {
    val ((author, ts, lat, lng), wcs) = awcs
    val nice_text = wcs.map((w: WordCount) => w._1 + ":" + w._2).mkString(" ")
    author + "\t" + ts + "\t" + lat + "," + lng + "\t" + nice_text
  }
  */

  def record_to_string(awcs: (AuthorWord, Long)): String = {
    val (((author, ts, lat, lng), word), count) = awcs
    author + "\t" + ts + "\t" + lat + "," + lng + "\t" + word + "\t" + count
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
    val values_extracted = lines.filter(is_valid_tweet).map(parse_json)

    // group by author, combine the records, keeping the earliest
    // tweet with a specific coordinate
    val concatted = values_extracted.groupByKey.combine(merge_records)

    // filter every user that doesn't have a specific coordinate
    val with_coord = concatted.filter(has_latlng)

    // word count
    val emitted_words = with_coord.flatMap(emit_words)
    val word_counts = emitted_words.groupByKey.combine((a: Long, b: Long) => (a + b))

    // regroup with author as key, word pairs as values
    //val regrouped_by_author = word_counts.map(reposition_word).groupByKey

    val nicely_formatted = word_counts.map(record_to_string)

    // save to disk
    DList.persist(TextOutput.toTextFile(nicely_formatted, outputPath))

  }
}

