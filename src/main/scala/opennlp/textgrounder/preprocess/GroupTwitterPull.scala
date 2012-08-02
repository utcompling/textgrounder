//  GroupTwitterPull.scala
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

import net.liftweb
import com.codahale.jerkson
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.testing.HadoopLogFactory
// import com.nicta.scoobi.application.HadoopLogFactory
import org.apache.hadoop.fs.{FileSystem=>HFileSystem,_}
import java.io._
import java.lang.Double.isNaN
import java.text.{SimpleDateFormat, ParseException}
import collection.JavaConversions._

import util.control.Breaks._

import opennlp.textgrounder.util.Twokenize
import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.ioutil._
import opennlp.textgrounder.util.osutil._
import opennlp.textgrounder.util.printutil._
import opennlp.textgrounder.gridlocate.DistDocument

/*
 * This program takes, as input, files which contain one tweet
 * per line in json format as directly pulled from the twitter
 * API. It combines the tweets either by user or by time, and outputs a
 * folder that may be used as the --input-corpus argument of tg-geolocate.
 * This is in "TextGrounder corpus" format, with one document per line,
 * fields separated by tabs, and all the ngrams and counts placed in a single
 * field, of the form "WORD1:WORD2:...:COUNT WORD1:WORD2:...:COUNT ...".
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

class GroupTwitterPullParams(ap: ArgParser) {
  // The following is set based on presence or absence of --by-time
  var keytype = "user"
  var timeslice_float = ap.option[Double]("timeslice", "time-slice",
    default = 6.0,
    help="""Number of seconds per timeslice when grouping '--by-time'.
    Can be a fractional number.  Default %default.""")
  // The following is set based on --timeslice
  var timeslice: Long = _
  var corpus_name = ap.option[String]("corpus-name", default = "unknown",
    help="""Name of corpus; for identification purposes.
    Default '%default'.""")
  var split = ap.option[String]("split", default = "training",
    help="""Split (training, dev, test) to place data in.  Default %default.""")
  var filter = ap.option[String]("filter",
    help="""Boolean expression used to filter tweets to be output.
Expression consists of one or more sequences of words, joined by the operators
AND, OR and NOT.  A sequence of words matches a tweet if and only if that exact
sequence is found in the tweet (tweets are matched after they have been
tokenized).  AND has higher precedence than OR.  Parentheses can be used
for grouping or precedence.  Any word that is quoted is treated as a literal
regardless of the characters in it; this can be used to treat words such as
"AND" literally.  Matching is case-insensitive; use '--cfilter' for
case-sensitive matching.  Note that the use of '--preserve-case' has no effect
on the case sensitivity of filtering; it rather affects whether the output
is converted to lowercase or left as-is.

Examples:

--filter "mitt romney OR obama"

Look for any tweets containing the sequence "mitt romney" (in any case) or
"Obama".

--filter "mitt AND romney OR barack AND obama"

Look for any tweets containing either the words "mitt" and "romney" (in any
case and anywhere in the tweet) or the words "barack" and "obama".

--filter "hillary OR bill AND clinton"

Look for any tweets containing either the word "hillary" or both the words
"bill" and "clinton" (anywhere in the tweet).

--filter "(hillary OR bill) AND clinton"

Look for any tweets containing the word "clinton" as well as either the words
"bill" or "hillary".""")
  var cfilter = ap.option[String]("cfilter",
    help="""Boolean expression used to filter tweets to be output, with
    case-sensitive matching.  Format is identical to '--filter'.""")
  var by_time = ap.flag("by-time",
    help="""Group tweets by time instead of by user.  When this is used, all
    tweets within a timeslice of a give number of seconds (specified using
    '--timeslice') are grouped together.""")
  var preserve_case = ap.flag("preserve-case",
    help="""Don't lowercase words.  This preserves the difference
    between e.g. the name "Mark" and the word "mark".""")
  var max_ngram = ap.option[Int]("max-ngram", "max-n-gram", "ngram", "n-gram",
    default = 1,
    help="""Largest size of n-grams to create.  Default 1, i.e. distribution
    only contains unigrams.""")
  var min_tweets = ap.option[Int]("min-tweets",
    default = 10,
    help="""Minimum number of tweets per user for user to be accepted in
    --by-user mode.""")
  var max_tweets = ap.option[Int]("max-tweets",
    default = 1000,
    help="""Maximum number of tweets per user for user to be accepted in
    --by-user mode.""")
  var debug = ap.flag("debug",
    help="""Output debug info about tweet processing/acceptance.""")
  var debug_file = ap.option[String]("debug-file",
    help="""File to write debug info to, instead of stderr.""")
  var use_jerkson = ap.flag("use-jerkson",
    help="""Use Jerkson instead of Lift to parse JSON.""")
  var input = ap.positional[String]("INPUT",
    help = "Source directory to read files from.")
  var output = ap.positional[String]("OUTPUT",
    help = "Destination directory to place files in.")
}

import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.input.CharArrayReader.EofCh

/**
 * A class used for filtering tweets using a boolean expression.
 * Parsing of the boolean expression uses Scala parsing combinators.
 *
 * To use, create an instance of this class; then call `parse` to
 * parse an expression into an abstract syntax tree object.  Then use
 * the `matches` method on this object to match against a tweet.
 */
class TweetFilterParser(foldcase: Boolean) extends StandardTokenParsers {
  sealed abstract class Expr {
    /**
     * Check if this expression matches the given sequence of words.
     */
    def matches(x: Seq[String]) =
      if (foldcase)
        check(x map (_.toLowerCase))
      else
        check(x)
    // Not meant to be called externally.
    def check(x: Seq[String]): Boolean
  }

  case class EConst(value: Seq[String]) extends Expr {
    def check(x: Seq[String]) = x containsSlice value
  }

  case class EAnd(left:Expr, right:Expr) extends Expr {
    def check(x: Seq[String]) = left.check(x) && right.check(x)
  }

  case class EOr(left:Expr, right:Expr) extends Expr {
    def check(x: Seq[String]) = left.check(x) || right.check(x)
  }

  case class ENot(e:Expr) extends Expr {
    def check(x: Seq[String]) = !e.check(x)
  }

  class ExprLexical extends StdLexical {
    override def token: Parser[Token] = floatingToken | super.token

    def floatingToken: Parser[Token] =
      rep1(digit) ~ optFraction ~ optExponent ^^
        { case intPart ~ frac ~ exp => NumericLit(
            (intPart mkString "") :: frac :: exp :: Nil mkString "")}

    def chr(c:Char) = elem("", ch => ch==c )
    def sign = chr('+') | chr('-')
    def optSign = opt(sign) ^^ {
      case None => ""
      case Some(sign) => sign
    }
    def fraction = '.' ~ rep(digit) ^^ {
      case dot ~ ff => dot :: (ff mkString "") :: Nil mkString ""
    }
    def optFraction = opt(fraction) ^^ {
      case None => ""
      case Some(fraction) => fraction
    }
    def exponent = (chr('e') | chr('E')) ~ optSign ~ rep1(digit) ^^ {
      case e ~ optSign ~ exp => e :: optSign :: (exp mkString "") :: Nil mkString ""
    }
    def optExponent = opt(exponent) ^^ {
      case None => ""
      case Some(exponent) => exponent
    }
  }

  class FilterLexical extends StdLexical {
    // see `token` in `Scanners`
    override def token: Parser[Token] =
      ( delim
      | unquotedWordChar ~ rep( unquotedWordChar )  ^^
         { case first ~ rest => processIdent(first :: rest mkString "") }
      | '\"' ~ rep( quotedWordChar ) ~ '\"' ^^
         { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
      | EofCh ^^^ EOF
      | '\"' ~> failure("unclosed string literal")
      | failure("illegal character")
      )

    def isPrintable(ch: Char) =
       !ch.isControl && !ch.isSpaceChar && !ch.isWhitespace && ch != EofCh
    def isPrintableNonDelim(ch: Char) =
       isPrintable(ch) && ch != '(' && ch != ')'
    def unquotedWordChar = elem("unquoted word char",
       ch => ch != '"' && isPrintableNonDelim(ch))
    def quotedWordChar = elem("quoted word char",
       ch => ch != '"' && ch != '\n' && ch != EofCh)

   // // see `whitespace in `Scanners`
   // def whitespace: Parser[Any] = rep(
   //     whitespaceChar
   // //  | '/' ~ '/' ~ rep( chrExcept(EofCh, '\n') )
   //   )

    override protected def processIdent(name: String) =
      if (reserved contains name) Keyword(name) else StringLit(name)
  }

  override val lexical = new FilterLexical
  lexical.reserved ++= List("AND", "OR", "NOT")
  // lexical.delimiters ++= List("&","|","!","(",")")
  lexical.delimiters ++= List("(",")")

  def word = stringLit ^^ {
    s => EConst(Seq(if (foldcase) s.toLowerCase else s))
  }

  def words = word.+ ^^ {
    x => EConst(x.flatMap(_ match { case EConst(y) => y }))
  }

  def parens: Parser[Expr] = "(" ~> expr <~ ")"

  def not: Parser[ENot] = "NOT" ~> term ^^ { ENot(_) }

  def term = ( words | parens | not )

  def andexpr = term * (
    "AND" ^^^ { (a:Expr, b:Expr) => EAnd(a,b) } )

  def orexpr = andexpr * (
    "OR" ^^^ { (a:Expr, b:Expr) => EOr(a,b) } )

  def expr = ( orexpr | term )

  def maybe_parse(s: String) = {
    val tokens = new lexical.Scanner(s)
    phrase(expr)(tokens)
  }

  def parse(s: String): Expr = {
    maybe_parse(s) match {
      case Success(tree, _) => tree
      case e: NoSuccess =>
        throw new IllegalArgumentException("Bad syntax: "+s)
    }
  }

  def test(exprstr: String, tweet: Seq[String]) = {
    maybe_parse(exprstr) match {
      case Success(tree, _) =>
        println("Tree: "+tree)
        val v = tree.matches(tweet)
        println("Eval: "+v)
      case e: NoSuccess => errprint("%s\n" format e)
    }
  }
  
  //A main method for testing
  def main(args: Array[String]) = test(args(0), args(1).split("""\s"""))
}

class ParseAndUniquifyTweets(Opts: GroupTwitterPullParams)
    extends GroupTwitterPullShared(Opts) {
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
   * An empty tweet, stored as a full IDRecord.
   */
  val empty_tweet: IDRecord = ("", ("", Tweet.empty))

  def parse_problem(line: String, e: Exception) = {
    dbg("Error parsing line: %s\n%s", line, e)
    empty_tweet
  }

  def force_value(value: liftweb.json.JValue): String = {
    if ((value values) == null)
      null
    else
      (value values) toString
  }

  /**
   * Parse a JSON line into a tweet, using Lift.
   */
  def parse_json_lift(line: String): IDRecord = {
    try {
      val parsed = liftweb.json.parse(line)
      val user = force_value(parsed \ "user" \ "screen_name")
      val timestamp = parse_time(force_value(parsed \ "created_at"))
      val text = force_value(parsed \ "text").replaceAll("\\s+", " ")
      val followers = force_value(parsed \ "user" \ "followers_count").toInt
      val following = force_value(parsed \ "user" \ "friends_count").toInt
      val tweet_id = force_value(parsed \ "id_str")
      val (lat, long) =
        if ((parsed \ "coordinates" values) == null ||
            (force_value(parsed \ "coordinates" \ "type") != "Point")) {
          (Double.NaN, Double.NaN)
        } else {
          val latlong: List[Number] =
            (parsed \ "coordinates" \ "coordinates" values).asInstanceOf[List[Number]]
          (latlong(1).doubleValue, latlong(0).doubleValue)
        }
      val key = Opts.keytype match {
        case "user" => user
        case _ => ((timestamp / Opts.timeslice) * Opts.timeslice).toString
      }
      (tweet_id, (key,
        Tweet(user, timestamp, text, lat, long, followers, following, 1)))
    } catch {
      case jpe: liftweb.json.JsonParser.ParseException =>
        parse_problem(line, jpe)
      case npe: NullPointerException =>
        parse_problem(line, npe)
      case nfe: NumberFormatException =>
        parse_problem(line, nfe)
      case e: Exception =>
        { parse_problem(line, e); throw e }
      case npe: NullPointerException => empty_tweet
    }
  }

  def get_string(value: Map[String, Any], l1: String) = {
    value(l1).asInstanceOf[String]
  }

  def get_2nd_level_value[T](value: Map[String, Any], l1: String,
      l2: String) = {
    value(l1).asInstanceOf[java.util.LinkedHashMap[String,Any]](l2).
      asInstanceOf[T]
  }

  /**
   * Parse a JSON line into a tweet, using Jerkson.
   */

  def parse_json_jerkson(line: String): IDRecord = {
    try {
      val parsed = jerkson.Json.parse[Map[String,Any]](line)
      val user = get_2nd_level_value[String](parsed, "user", "screen_name")
      val timestamp = parse_time(get_string(parsed, "created_at"))
      val text = get_string(parsed, "text").replaceAll("\\s+", " ")
      val followers = get_2nd_level_value[Int](parsed, "user", "followers_count")
      val following = get_2nd_level_value[Int](parsed, "user", "friends_count")
      val tweet_id = get_string(parsed, "id_str")
      val (lat, long) =
        if (parsed("coordinates") == null ||
            get_2nd_level_value[String](parsed, "coordinates", "type")
              != "Point") {
          (Double.NaN, Double.NaN)
        } else {
          val latlong =
            get_2nd_level_value[java.util.ArrayList[Number]](parsed,
              "coordinates", "coordinates")
          (latlong(1).doubleValue, latlong(0).doubleValue)
        }
      val key = Opts.keytype match {
        case "user" => user
        case _ => ((timestamp / Opts.timeslice) * Opts.timeslice).toString
      }
      (tweet_id, (key,
        Tweet(user, timestamp, text, lat, long, followers, following, 1)))
    } catch {
      case jpe: jerkson.ParsingException =>
        parse_problem(line, jpe)
      case npe: NullPointerException =>
        parse_problem(line, npe)
      case nfe: NumberFormatException =>
        parse_problem(line, nfe)
      case nsee: NoSuchElementException =>
        parse_problem(line, nsee)
      case e: Exception =>
        { parse_problem(line, e); throw e }
    }
  }

  /*
   * Parse a JSON line into a tweet.  Return value is an IDRecord, including
   * the tweet ID, username, text and all other data.
   */
  def parse_json(line: String): IDRecord = {
    // For testing
    // dbg("parsing JSON: %s", line)
    if (line.trim == "")
      empty_tweet
    else if (Opts.use_jerkson)
      parse_json_jerkson(line)
    else
      parse_json_lift(line)
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
    val (tw_id, (key, tw)) = id_r
    tw_id != "" && tw.timestamp != 0 && tw.user != "" &&
      !(tw.lat == 0.0 && tw.long == 0.0)
  }

  /**
   * Select the first tweet with the same ID.  For various reasons we may
   * have duplicates of the same tweet among our data.  E.g. it seems that
   * Twitter itself sometimes streams duplicates through its Streaming API,
   * and data from different sources will almost certainly have duplicates.
   * Furthermore, sometimes we want to get all the tweets even in the presence
   * of flakiness that causes Twitter to sometimes bomb out in a Streaming
   * session and take a while to restart, so we have two or three simultaneous
   * streams going recording the same stuff, hoping that Twitter bombs out at
   * different points in the different sessions (which is generally true).
   * Then, all or almost all the tweets are available in the different streams,
   * but there is a lot of duplication that needs to be tossed aside.
   */
  def tweet_once(id_rs: (TweetID, Iterable[Record])): Record = {
    val (id, rs) = id_rs
    rs.head
  }
}

object ParseAndUniquifyTweets {
  def apply(Opts: GroupTwitterPullParams, lines: DList[String]) = {
    val obj = new ParseAndUniquifyTweets(Opts)

    // Parse JSON into tweet records (IDRecord), filter out invalid tweets.
    val values_extracted = lines.map(obj.parse_json).filter(obj.is_valid_tweet)

    // Filter out duplicate tweets -- group by Tweet ID and then take the
    // first tweet for a given ID.  Duplicate tweets occur for various
    // reasons, e.g. sometimes in the stream itself due to Twitter errors,
    // or when combining data from multiple, possibly overlapping, sources.
    // In the process, the tweet ID's are discarded.
    val single_tweets = values_extracted.groupByKey.map(obj.tweet_once)

    // Checkpoint the resulting tweets (minus ID) onto disk.
    single_tweets.map(obj.checkpoint_str)
  }
}

class GroupTweetsAndSelectGood(Opts: GroupTwitterPullParams)
    extends GroupTwitterPullShared(Opts) {
  /**
   * Convert a checkpointed string generated by `checkpoint_str` back into
   * the record it came from.
   */
  def from_checkpoint_to_record(line: String): Record = {
    val split = line.split("\t\t")
    assert(split.length == 2)
    val (tweet_no_text, text) = (split(0), split(1))
    val (key, tnt) = split_tweet_no_text(tweet_no_text)
    (key, Tweet(tnt.user, tnt.timestamp, text, tnt.lat, tnt.long,
                tnt.followers, tnt.following, tnt.numtweets))
  }

  /**
   * Merge the data associated with two tweets or tweet combinations
   * into a single tweet combination.  Concatenate text.  Find maximum
   * numbers of followers and followees.  Add number of tweets in each.
   * For latitude and longitude, take the earliest provided values
   * ("earliest" by timestamp and "provided" meaning not missing).
   */
  def merge_records(tw1: Tweet, tw2: Tweet): Tweet = {
    val (followers, following) =
      (math.max(tw1.followers, tw2.followers),
       math.max(tw1.following, tw2.following))
    val text = tw1.text + " " + tw2.text
    val numtweets = tw1.numtweets + tw2.numtweets

    val (lat, long, timestamp) =
      if (isNaN(tw1.lat) && isNaN(tw2.lat)) {
        (tw1.lat, tw1.long, math.min(tw1.timestamp, tw2.timestamp))
      } else if (isNaN(tw2.lat)) {
        (tw1.lat, tw1.long, tw1.timestamp)
      } else if (isNaN(tw1.lat)) {
        (tw2.lat, tw2.long, tw2.timestamp)
      } else if (tw1.timestamp < tw2.timestamp) {
        (tw1.lat, tw1.long, tw1.timestamp)
      } else {
        (tw2.lat, tw2.long, tw2.timestamp)
      }

    // FIXME maybe want to track the different users
    Tweet(tw1.user, timestamp, text, lat, long, followers, following,
      numtweets)
  }

  /**
   * Return true if tweet (combination) has a fully-specified latitude
   * and longitude.
   */
  def has_latlong(r: Record) = {
    val (key, tw) = r
    !isNaN(tw.lat) && !isNaN(tw.long)
  }

  val MAX_NUMBER_FOLLOWING = 1000
  val MIN_NUMBER_FOLLOWING = 5
  val MIN_NUMBER_FOLLOWERS = 10
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
    val (key, tw) = r

    val retval =
      (tw.following >= MIN_NUMBER_FOLLOWING &&
         tw.following <= MAX_NUMBER_FOLLOWING) &&
      (tw.followers >= MIN_NUMBER_FOLLOWERS) &&
      (tw.numtweets >= Opts.min_tweets && tw.numtweets <= Opts.max_tweets)
    if (Opts.debug && retval == false)
      dbg("Rejecting is_nonspammer %s", r)
    retval
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
    val (key, tw) = r
    val retval = (tw.lat >= MIN_LAT && tw.lat <= MAX_LAT) &&
                 (tw.long >= MIN_LNG && tw.long <= MAX_LNG)
    if (Opts.debug && retval == false)
      dbg("Rejecting northamerica_only %s", r)
    retval
  }

  def is_good_tweet(r: Record): Boolean = {
    if (Opts.debug)
      dbg("Considering %s", r)
    has_latlong(r) &&
    is_nonspammer(r) &&
    northamerica_only(r)
  }
}

object GroupTweetsAndSelectGood {
  def apply(Opts: GroupTwitterPullParams, lines2: DList[String]) = {
    val obj = new GroupTweetsAndSelectGood(Opts)

    val values_extracted2 = lines2.map(obj.from_checkpoint_to_record)

    // Group by username, then combine the tweets for a user into a
    // tweet combination, with text concatenated and the location taken
    // from the earliest/ tweet with a specific coordinate.
    val concatted = values_extracted2.groupByKey.combine(obj.merge_records)

    // If grouping by user, filter the tweet combinations, removing users
    // without a specific coordinate; users that appear to be "spammers" or
    // other users with non-standard behavior; and users located outside of
    // North America.  FIXME: We still want to filter spammers; but this
    // is trickier when not grouping by user.  How to do it?
    val good_tweets =
      if (Opts.keytype == "timestamp") concatted
      else concatted.filter(obj.is_good_tweet)

    // Checkpoint a second time.
    good_tweets.map(obj.checkpoint_str)
  }
}

class TokenizeFilterAndCountTweets(Opts: GroupTwitterPullParams)
    extends GroupTwitterPullShared(Opts) {
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
   * Convert a word to lowercase.
   */
  def normalize_word(orig_word: String) = {
    val word =
      if (Opts.preserve_case)
        orig_word
      else
        orig_word.toLowerCase
    // word.startsWith("@")
    if (word.contains("http://") || word.contains("https://"))
      "-LINK-"
    else
      word
  }

  /**
   * Return true if word should be filtered out (post-normalization).
   */
  def reject_word(word: String) = {
    word == "-LINK-"
  }

  /**
   * Return true if ngram should be filtered out (post-normalization).
   * Here we filter out things where every word should be filtered, or
   * where the first or last word should be filtered (in such a case, all
   * the rest will be contained in a one-size-down n-gram).
   */
  def reject_ngram(ngram: Iterable[String]) = {
    ngram.forall(reject_word) || reject_word(ngram.head) ||
      reject_word(ngram.last)
  }

  def create_parser(expr: String, foldcase: Boolean) = {
    if (expr == null) null
    else new TweetFilterParser(foldcase).parse(expr)
  }

  lazy val filter_ast = create_parser(Opts.filter, foldcase = true)
  lazy val cfilter_ast = create_parser(Opts.cfilter, foldcase = false)

  /**
   * Use Twokenize to break up a tweet into tokens, filter it if necessary
   * according to --filter and --cfilter, and separate into ngrams.
   */
  def break_tweet_into_ngrams(text: String):
      Iterable[Iterable[String]] = {
    val words = Twokenize(text)
    if ((filter_ast == null || (filter_ast matches words)) &&
        (cfilter_ast == null || (cfilter_ast matches words))) {
      val normwords = words.map(normalize_word)

      // Then, generate all possible ngrams up to a specified maximum length,
      // where each ngram is a sequence of words.  `sliding` overlays a sliding
      // window of a given size on a sequence to generate successive
      // subsequences -- exactly what we want to generate ngrams.  So we
      // generate all 1-grams, then all 2-grams, etc. up to the maximum size,
      // and then concatenate the separate lists together (that's what `flatMap`
      // does).
      (1 to Opts.max_ngram).
        flatMap(normwords.sliding(_)).filter(!reject_ngram(_))
    } else
      Iterable[Iterable[String]]()
  }

  /**
   * Given the tweet data minus the text field combined into a string,
   * plus the text field, tokenize the text and emit the ngrams individually.
   * Each ngram is emitted along with the text data and a count of 1,
   * and later grouping + combining will add all the 1's to get the
   * ngram count.
   */
  def emit_ngrams(tweet_text: (String, String)):
      Iterable[(TweetNgram, Long)] = {
    val (tweet_no_text, text) = tweet_text
    for (ngram <- break_tweet_into_ngrams(text))
      yield ((tweet_no_text, DistDocument.encode_ngram_for_counts_field(ngram)),
             1L)
  }

  /**
   * We made the value in the key-value pair be a count so we can combine
   * the counts easily to get the total ngram count, and stuffed all the
   * rest of the data (tweet data plus ngram) into the key, but now we have to
   * rearrange to move the ngram back into the value.
   */
  def reposition_ngram(tnc: (TweetNgram, Long)): (String, NgramCount) = {
    val ((tweet_no_text, ngram), c) = tnc
    (tweet_no_text, (ngram, c))
  }

  /**
   * Given tweet data minus text plus an iterable of ngram-count pairs,
   * convert to a string suitable for outputting.
   */
  def nicely_format_plain(tncs: (String, Iterable[NgramCount])): String = {
    val (tweet_no_text, ncs) = tncs
    val nice_text = ncs.map((w: NgramCount) => w._1 + ":" + w._2).mkString(" ")
    val (key, tnt) = split_tweet_no_text(tweet_no_text)
    // Latitude/longitude need to be combined into a single field, but only
    // if both actually exist.
    val latlongstr =
      if (!isNaN(tnt.lat) && !isNaN(tnt.long))
        "%s,%s" format (tnt.lat, tnt.long)
      else ""
    // Put back together but drop key.
    Seq(tnt.user, tnt.timestamp, latlongstr, tnt.followers, tnt.following,
        tnt.numtweets, nice_text) mkString "\t"
  }
}

object TokenizeFilterAndCountTweets {
  def apply(Opts: GroupTwitterPullParams, lines_cp: DList[String]) = {
    val obj = new TokenizeFilterAndCountTweets(Opts)

    // Now count ngrams.  We run `from_checkpoint_to_tweet_text` (see above)
    // to get separate access to the text, then Twokenize it into words,
    // generate ngrams from them and emit a series of key-value pairs of
    // (ngram, count).
    val emitted_ngrams = lines_cp.map(obj.from_checkpoint_to_tweet_text)
                                 .flatMap(obj.emit_ngrams)
    // Group based on key (the ngram) and combine by adding up the individual
    // counts.
    val ngram_counts =
      emitted_ngrams.groupByKey.combine((a: Long, b: Long) => a + b)

    // Regroup with proper key (user, timestamp, etc.) as key,
    // ngram pairs as values.
    val regrouped_by_key = ngram_counts.map(obj.reposition_ngram).groupByKey

    // Nice string output.
    regrouped_by_key.map(obj.nicely_format_plain)
  }
}

/**
 * Data for a tweet other than the tweet ID.
 * Note that we have "number of tweets" since we merge multiple tweets into
 * a document, and also use type Tweet or TweetNoText for them. */
case class Tweet(
  user: String,
  timestamp: Long,
  text: String,
  lat: Double,
  long: Double,
  followers: Int,
  following: Int,
  numtweets: Int
)

object Tweet {
  def empty = Tweet("", 0, "", Double.NaN, Double.NaN, 0, 0, 0)
  // Not needed unless we have Tweet inside of a DList, it seems?
  // But we do, in intermediate results?
  // implicit val tweetFmt = mkCaseWireFormat(Tweet.apply _, Tweet.unapply _)
}

/**
 * Data for a merged set of tweets other than the text.
 *
 * @param user User name (FIXME: or one of them, when going by time; should
 *    do something smarter)
 * @param timestamp Earliest timestamp
 * @param lat Best latitude
 * @param long Best longitude
 * @param followers Max followers
 * @param following Max following
 * @param numtweets Number of tweets merged
 */
case class TweetNoText(
  user: String,
  timestamp: Long,
  lat: Double,
  long: Double,
  followers: Int,
  following: Int,
  numtweets: Int
)
object TweetNoText {
  // implicit val tweetNoTextFmt =
  //   mkCaseWireFormat(TweetNoText.apply _, TweetNoText.unapply _)
}

// type TweetNoText = (String, Long, Double, Double, Int, Int, Int)
// TweetNgram = Data for the tweet minus the text, plus an individual ngram
//   from the text = (tweet_no_text_as_string, ngram)

class GroupTwitterPullShared(Opts: GroupTwitterPullParams) {
  // type Tweet = (String, Long, String, Double, Double, Int, Int, Int)
  // TweetID = numeric string used to uniquely identify a tweet.
  type TweetID = String

  //case class Record(
  //  key: String,
  //  tweet: Tweet
  //)
  //implicit val recordFmt = mkCaseWireFormat(Record, Record.unapply _)

  // Record = Data for tweet along with the key (e.g. username, timestamp) =
  // (key, tweet data)
  type Record = (String, Tweet)
  // IDRecord = Tweet ID along with all other data for a tweet.
  type IDRecord = (TweetID, Record)
  type TweetNgram = (String, String)
  // NgramCount = (ngram, number of ocurrences)
  type NgramCount = (String, Long)

  def dbg(format: String, args: Any*) {
    errfile(Opts.debug_file, format, args: _*)
  }

  /**
   * Convert a "record" (key plus tweet data) into a line of text suitable
   * for writing to a "checkpoint" file.  We encode all the fields into text
   * and separate by tabs.  The text gets moved to the end and preceded by
   * two tabs, so it can be found more easily when re-reading. FIXME: This
   * is fragile; will have problems if any field is blank.  Just peel off
   * the last field, or whatever.
   */
  def checkpoint_str(r: Record): String = {
    val (key, tw) = r
    val text_2 = tw.text.replaceAll("\\s+", " ")
    val s = Seq(key, tw.user, tw.timestamp, tw.lat, tw.long, tw.followers,
      tw.following, tw.numtweets, "", text_2) mkString "\t"
    s
  }

  def split_tweet_no_text(tweet_no_text: String): (String, TweetNoText) = {
    val split2 = tweet_no_text.split("\t")
    val key = split2(0)
    val user = split2(1)
    val timestamp = split2(2).toLong
    val lat = split2(3).toDouble
    val long = split2(4).toDouble
    val followers = split2(5).toInt
    val following = split2(6).toInt
    val numtweets = split2(7).toInt
    (key, TweetNoText(user, timestamp, lat, long, followers, following,
      numtweets))
  }
}

class GroupTwitterPull(Opts: GroupTwitterPullParams)
    extends GroupTwitterPullShared(Opts) {
  def corpus_suffix = {
    val dist_type = if (Opts.max_ngram == 1) "unigram" else "ngram"
    "%s-%s-counts" format (Opts.split, dist_type)
  }

  /**
   * Output a schema file of the appropriate name.
   */
  def output_schema(fs: HFileSystem) {
    val filename =
      "%s/%s-%s-schema.txt" format
        (Opts.output, Opts.corpus_name, corpus_suffix)
    errprint("Outputting a schema to %s ...", filename)
    val p = new PrintWriter(fs.create(new Path(filename)))
    def print_seq(s: String*) {
      p.println(s mkString "\t")
    }
    try {
      print_seq("user", "timestamp", "coord", "followers", "following",
        "numtweets", "counts")
      print_seq("corpus", Opts.corpus_name)
      print_seq("corpus-type", "twitter-%s" format Opts.keytype)
      if (Opts.keytype == "timestamp")
        print_seq("corpus-timeslice", Opts.timeslice.toString)
      print_seq("split", Opts.split)
    } finally { p.close() }
  }

  def output_command_line_parameters(arg_parser: ArgParser) {
    dbg("")
    dbg("Non-default parameter values:")
    for (name <- arg_parser.argNames) {
      if (arg_parser.specified(name))
        dbg("%30s: %s", name, arg_parser(name))
    }
    dbg("")
    dbg("Parameter values:")
    for (name <- arg_parser.argNames) {
      dbg("%30s: %s", name, arg_parser(name))
      //dbg("%30s: %s", name, arg_parser.getType(name))
    }
    dbg("")
  }
}

object GroupTwitterPull extends ScoobiApp {
  def run() {
    initialize_osutil()
    val ap = new ArgParser("GroupTwitterPull")
    // This first call is necessary, even though it doesn't appear to do
    // anything.  In particular, this ensures that all arguments have been
    // defined on `ap` prior to parsing.
    new GroupTwitterPullParams(ap)
    errprint("Parsing args: %s", args mkString " ")
    ap.parse(args)
    val Opts = new GroupTwitterPullParams(ap)
    if (Opts.by_time)
      Opts.keytype = "timestamp"
    Opts.timeslice = (Opts.timeslice_float * 1000).toLong
    if (Opts.debug)
      HadoopLogFactory.setQuiet(false)
    if (Opts.debug_file != null)
      set_errout_file(Opts.debug_file)

    val ptp = new GroupTwitterPull(Opts)
    ptp.output_command_line_parameters(ap)

    errprint("Step 1: Load up the JSON tweets, parse into records, remove duplicates, checkpoint.")
    // Firstly we load up all the (new-line-separated) JSON lines.
    val lines: DList[String] = TextInput.fromTextFile(Opts.input)

    val checkpoint1 = ParseAndUniquifyTweets(Opts, lines)
    persist(TextOutput.toTextFile(checkpoint1, Opts.output + "-st"))
    errprint("Step 1: done.")

    // Then load back up.
    errprint("Step 2: Load parsed tweets, group, filter bad results.")
    if (Opts.by_time)
      errprint("        (grouping by time, with slices of %g seconds)",
        Opts.timeslice_float)
    else
      errprint("        (grouping by user)")
    val lines2: DList[String] = TextInput.fromTextFile(Opts.output + "-st")
    val checkpoint = GroupTweetsAndSelectGood(Opts, lines2)
    persist(TextOutput.toTextFile(checkpoint, Opts.output + "-cp"))
    errprint("Step 2: done.")

    // Load from second checkpoint.  Note that each time we checkpoint,
    // we extract the "text" field and stick it at the end.  This time
    // when loading it up, we use `from_checkpoint_to_tweet_text`, which
    // gives us the tweet data as a string (minus text) and the text, as
    // two separate strings.
    errprint("Step 3: Load grouped tweets, tokenize, counts ngrams, output corpus.")
    val lines_cp: DList[String] = TextInput.fromTextFile(Opts.output + "-cp")

    val nicely_formatted = TokenizeFilterAndCountTweets(Opts, lines_cp)

    // Save to disk.
    persist(TextOutput.toTextFile(nicely_formatted, Opts.output))
    errprint("Step 3: done.")

    // Rename output files appropriately
    errprint("Renaming output files ...")
    val fs = configuration.fs
    val globpat = "%s/*-r-*" format Opts.output
    for (file <- fs.globStatus(new Path(globpat))) {
      val path = file.getPath
      val basename = path.getName
      val newname = "%s/%s-%s-%s.txt" format (
        Opts.output, Opts.corpus_name, basename, ptp.corpus_suffix)
      errprint("Renaming %s to %s", path, newname)
      fs.rename(path, new Path(newname))
    }

    // create a schema
    ptp.output_schema(fs)

    errprint("All done with everything.")
    errprint("")
    output_resource_usage()
  }
}

