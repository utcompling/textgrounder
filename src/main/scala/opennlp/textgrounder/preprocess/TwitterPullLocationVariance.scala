package opennlp.textgrounder.preprocess

import net.liftweb.json
import com.nicta.scoobi.Scoobi._
import java.io._
import java.lang.Double.isNaN
import java.text.{SimpleDateFormat, ParseException}
import math.pow

import opennlp.textgrounder.util.Twokenize
import opennlp.textgrounder.util.distances.{spheredist, SphereCoord}

/*
 * This program takes, as input, files which contain one tweet
 * per line in json format as directly pulled from the twitter
 * api. It outputs a folder that may be used as the
 * --input-corpus argument of tg-geolocate.
 */

object TwitterPullLocationVariance extends ScoobiApp {
  type Tweet = (Double, Double)
  type Record = (String, Tweet)

  def force_value(value: json.JValue): String = {
    if ((value values) == null)
        null
    else
        (value values) toString
  }

  def is_valid_tweet(id_r: (String, Record)): Boolean = {
    // filters out invalid tweets, as well as trivial spam
    val (tw_id, (a, (lat, lng))) = id_r
    a != "" && tw_id != "" && !isNaN(lat) && !isNaN(lng)
  }

  def preferred_latlng(latlng1: (Double, Double), latlng2: (Double, Double)): (Double, Double) = {
    val (lat1, lng1) = latlng1
    val (lat2, lng2) = latlng2
    if (!isNaN(lat1) && !isNaN(lng1))
      (lat1, lng1)
    else
      (lat2, lng2)
  }

  val empty_tweet: (String, Record) = ("", ("", (Double.NaN, Double.NaN)))

  def parse_json(line: String): (String, Record) = {
    try {
      val parsed = json.parse(line)
      val author = force_value(parsed \ "user" \ "screen_name")
      val tweet_id = force_value(parsed \ "id_str")
      val (blat, blng) = 
        try {
          val bounding_box = (parsed \ "place" \ "bounding_box" \ "coordinates" values)
                              .asInstanceOf[List[List[List[Double]]]](0)
          val bounding_box_sum = bounding_box.reduce((a, b) => List(a(1) + b(1), a(0) + b(0)))
          (bounding_box_sum(0) / bounding_box.length, bounding_box_sum(1) / bounding_box.length)
        } catch {
          case npe: NullPointerException => (Double.NaN, Double.NaN)
          case cce: ClassCastException => (Double.NaN, Double.NaN)
        }
      val (plat, plng) = 
        if ((parsed \ "coordinates" values) == null ||
            (force_value(parsed \ "coordinates" \ "type") != "Point")) {
          (Double.NaN, Double.NaN)
        } else {
          val latlng: List[Number] = 
            (parsed \ "coordinates" \ "coordinates" values).asInstanceOf[List[Number]]
          (latlng(1).doubleValue, latlng(0).doubleValue)
        }
      val (lat, lng) = preferred_latlng((plat, plng), (blat, blng))
      (tweet_id, (author, (lat, lng)))
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

  def has_latlng(r: Record): Boolean = {
    val (a, (lat, lng)) = r
    !isNaN(lat) && !isNaN(lng)
  }
  
  def cartesian_product[T1, T2](A: Seq[T1], B: Seq[T2]): Iterable[(T1, T2)] = {
    for (a <- A; b <- B) yield (a, b)
  }

  def mean_variance_and_maxdistance(inpt: (String, Iterable[(Double, Double)])):
      // Author, AvgLat, AvgLng, AvgDistance, DistanceVariance, MaxDistance
      (String, Double, Double, Double, Double, Double) = {
    val (author, latlngs_i) = inpt
    val latlngs = latlngs_i.toSeq
    val lats = latlngs.map(_._1)
    val lngs = latlngs.map(_._2)

    val avgpoint = SphereCoord(lats.sum / lats.length, lngs.sum / lngs.length)
    val allpoints = latlngs.map(ll => SphereCoord(ll._1, ll._2))
    val distances = allpoints.map(spheredist(_, avgpoint))
    val avgdistance = distances.sum / distances.length
    val distancevariance = distances.map(x => pow(x - avgdistance, 2)).sum / distances.length

    val maxdistance = cartesian_product(allpoints, allpoints)
                        .map{case (a, b) => spheredist(a, b)}.max

    (author, avgpoint.lat, avgpoint.long, avgdistance, distancevariance, maxdistance)
  }

  def nicely_format(r: (String, Double, Double, Double, Double, Double)): String = {
    val (a, b, c, d, e, f) = r
    Seq(a, b, c, d, e, f) mkString "\t"
  }

  def checkpoint_str(r: Record): String = {
    val (a, (lat, lng)) = r
    a + "\t" + lat + "\t" + lng
  }

  def from_checkpoint_to_record(s: String): Record = {
    val s_a = s.split("\t")
    (s_a(0), (s_a(1).toDouble, s_a(2).toDouble))
  }

  def run() {
    val (inputPath, outputPath) =
      if (args.length == 2) {
        (args(0), args(1))
      } else {
        sys.error("Expecting input and output path.")
      }

    /*
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    val values_extracted = lines.map(parse_json).filter(is_valid_tweet)
    val single_tweets = values_extracted.groupByKey.map(tweet_once)
                                                   .filter(has_latlng)

    val checkpointed = single_tweets.map(checkpoint_str)
    persist(TextOutput.toTextFile(checkpointed, inputPath + "-st"))
    */

    val single_tweets_lines: DList[String] = TextInput.fromTextFile(inputPath + "-st")
    val single_tweets_reloaded = single_tweets_lines.map(from_checkpoint_to_record)
    val grouped_by_author = single_tweets_reloaded.groupByKey

    val averaged = grouped_by_author.map(mean_variance_and_maxdistance)

    val nicely_formatted = averaged.map(nicely_format)
    persist(TextOutput.toTextFile(nicely_formatted, outputPath))
  }
}

