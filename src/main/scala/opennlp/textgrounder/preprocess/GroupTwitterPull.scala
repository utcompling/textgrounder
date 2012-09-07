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

import collection.JavaConversions._

import java.io._
import java.lang.Double.isNaN
import java.text.{SimpleDateFormat, ParseException}

import net.liftweb
import org.apache.commons.logging
import org.apache.hadoop.fs.{FileSystem=>HFileSystem,_}

import com.nicta.scoobi.Scoobi._

import opennlp.textgrounder.{util => tgutil}
import tgutil.Twokenize
import tgutil.argparser._
import tgutil.collectionutil._
import tgutil.corpusutil._
import tgutil.ioutil.FileHandler
import tgutil.hadoop.HadoopFileHandler
import tgutil.printutil._
import tgutil.timeutil._

class GroupTwitterPullParams(ap: ArgParser) extends
    ScoobiProcessFilesParams(ap) {
  var grouping = ap.option[String]("grouping", "g", "gr", "group",
    choices = Seq("user", "time", "none"),
    default = "user",
    help="""Mode for grouping tweets.  There are currently three methods
    of grouping: `user`, `time` (i.e. all tweets within a given
    timeslice, specified with `--timeslice`) and `none` (no grouping;
    tweets are passed through directly, after duplicated tweets have been
    removed).""")
  var output_format = ap.option[String]("output-format", "of",
    choices = Seq("corpus", "raw"),
    default = "corpus",
    help="""Format for output of tweets or tweet groups.  Possibilities are
    `corpus` (store in a TextGrounder-style corpus, i.e. as a simple database
    with one record per line, fields separated by TAB characters, and a
    schema indicating the names of the columns); and `raw` (simply output
    JSON-formatted tweets directly, exactly as received; only possible for
    `--grouping=none`, in which case the input tweets will be output
    directly, after removing duplicates.""")
  var corpus_name = ap.option[String]("corpus-name",
    help="""Name of output corpus; for identification purposes.
    Default to name taken from input directory.""")
  var split = ap.option[String]("split", default = "training",
    help="""Split (training, dev, test) to place data in.  Default %default.""")
  var timeslice_float = ap.option[Double]("timeslice", "time-slice",
    default = 6.0,
    help="""Number of seconds per timeslice when `--grouping=time`.
    Can be a fractional number.  Default %default.""")
  // The following is set based on --timeslice
  var timeslice: Long = _
  var filter_tweets = ap.option[String]("filter-tweets",
    help="""Boolean expression used to filter tweets to be output.
Expression consists of one or more expressions, joined by the operators
AND, OR and NOT (which must be written all-caps to be recognized).  The
order of precedence (from high to low) is

-- comparison operators (<, <=, >, >=, WITHIN)
-- NOT
-- AND
-- OR

Parentheses can be used for grouping or precedence.  Expressions consist of
one of the following:

-- A sequence of words, which matches a tweet if and only if that exact
   sequence is found in the tweet.  Matching happens on the word-by-word
   level, after a tweet has been tokenized.  Matching is case-insensitive;
   use '--cfilter-tweets' for case-sensitive matching.  Note that the use of
   '--preserve-case' has no effect on the case sensitivity of filtering;
   it rather affects whether the output is converted to lowercase or
   left as-is.  Any word that is quoted is treated as a literal
   regardless of the characters in it; this can be used to treat words such
   as "AND" literally.

-- An expression specifying a one-sided restriction on the time of the tweet,
   such as 'TIME < 20100802180502PDT' (earlier than August 2, 2010, 18:05:02
   Pacific Daylight Time) or 'TIME >= 2011:12:25:0905pm (at least as late as
   December 25, 2011, 9:05pm local time). The operators can be <, <=, > or >=.
   As for the time, either 12-hour or 24-hour time can be given, colons can
   optionally be inserted anywhere for readability, the time zone can be
   omitted or specified, and part or all of the time of day (hours, minutes,
   seconds) can be omitted.  Years must always be full (i.e. 4 digits).

-- An expression specifying a two-sided restriction on the time of the tweet
   (i.e. the tweet's time must be within a given interval).  Either of the
   following forms are allowed:
   
   -- 'TIME WITHIN 2010:08:02:1805PDT/2h'
   -- 'TIME WITHIN (2010:08:02:0500pmPDT 2010:08:03:0930amPDT)'
   
   That is, the interval of time can be given either as a point of time plus
   an offset, or as two points of time.  The offset can be specified in
   various ways, e.g.
   
   -- '1h' or '+1h' (1 hour)
   -- '3m2s' or '3m+2s' or '+3m+2s' (3 minutes, 2 seconds)
   -- '2.5h' (2.5 hours, i.e. 2 hours 30 minutes)
   -- '5d2h30m' (5 days, 2 hours, 30 minutes)
   -- '-3h' (-3 hours, i.e. 3 hours backwards from a given point of time)
   -- '5d-1s' (5 days less 1 second)
  
   That is, an offset is a combination of individual components, each of
   which is a number (possibly fractional or negative or with a prefixed
   plus sign, which is ignored) plus a unit: 'd' = days, 'h' = hours,
   'm' = minutes, 's' = seconds.  Negative offsets are allowed, to indicate
   an interval backwards from a reference point.

Examples:

--filter-tweets "mitt romney OR obama"

Look for any tweets containing the sequence "mitt romney" (in any case) or
"Obama".

--filter-tweets "mitt AND romney OR barack AND obama"

Look for any tweets containing either the words "mitt" and "romney" (in any
case and anywhere in the tweet) or the words "barack" and "obama".

--filter-tweets "hillary OR bill AND clinton"

Look for any tweets containing either the word "hillary" or both the words
"bill" and "clinton" (anywhere in the tweet).

--filter-tweets "(hillary OR bill) AND clinton"

Look for any tweets containing the word "clinton" as well as either the words
"bill" or "hillary".""")
  var cfilter_tweets = ap.option[String]("cfilter-tweets",
    help="""Boolean expression used to filter tweets to be output, with
    case-sensitive matching.  Format is identical to `--filter-tweets`.""")
  var filter_users = ap.option[String]("filter-users",
    help="""Boolean expression used to filter on the user level; only
  applicable with --grouping=user.  This is like `--filter-tweets` but
  filters users in such a way that all users with *any* tweet matching
  the filter will be passed through.""")
  var cfilter_users = ap.option[String]("cfilter-users",
    help="""Same as `--filter-users` but does case-sensitive matching.""")
  var geographic_only = ap.flag("geographic-only", "geog",
    help="""Filter out tweets that don't have a geotag or that have a
geotag outside of North America.  Also filter on min/max-followers, etc.""")
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

  override def check_usage() {
    timeslice = (timeslice_float * 1000).toLong
    if (output_format == "raw" && grouping != "none")
      ap.usageError("`raw` output format only allowed when `--grouping=none`")
  }
}

object GroupTwitterPull extends ScoobiProcessFilesApp[GroupTwitterPullParams] {
  /*
   * This program takes, as input, files which contain one tweet per line
   * in JSON format as directly pulled from the twitter API. It groups the
   * tweets either by user, by time or not at all, and outputs a folder that
   * may be used as the --input-corpus argument of tg-geolocate.
   * This is in "TextGrounder corpus" format, with one document per line
   * and fields separated by tabs.  Maps that specify the count of strings
   * are in the format "STRING:COUNT STRING:COUNT ...", where URL encoding is
   * used to encode characters that cannot be output directly (e.g. %20 for
   * a space, %25 for a % sign).  Ngram maps are of the form
   * "WORD1:WORD2:...:COUNT WORD1:WORD2:...:COUNT ...".
   *
   * When doing no grouping, output can also be in JSON format, using the
   * option `--output-format=raw`.
   *
   * When merging, the code uses the earliest geolocated tweet as the combined
   * location; tweets with a bounding box as their location rather than
   * a single point are treated as if they have no location.
   *
   * When grouping by user, the code filters out users that have no geolocation
   * or have a geolocation outside of North America (according to a crude
   * bounding box), as well as those that are identified as likely "spammers"
   * according to their number of followers and number of people they are
   * following.
   */

  /**
   * Data for a tweet or grouping of tweets, other than the tweet ID.
   */
  case class Tweet(
    text: Seq[String],
    notext: TweetNoText
  )

  object Tweet {
    def empty = Tweet(Seq[String](), TweetNoText.empty)
  }

  /**
   * Data for a merged set of tweets other than the text.
   *
   * @param user User name (FIXME: or one of them, when going by time; should
   *    do something smarter)
   * @param min_timestamp Earliest timestamp
   * @param max_timestamp Latest timestamp
   * @param geo_timestamp Earliest timestamp of tweet with corresponding
   *    location
   * @param lat Best latitude (corresponding to the earliest tweet)
   * @param long Best longitude (corresponding to the earliest tweet)
   * @param followers Max followers
   * @param following Max following
   * @param numtweets Number of tweets merged
   * @param user_mentions Item-count map of all @-mentions
   * @param retweets Like `user_mentions` but only for retweet mentions
   * @param hashtags Item-count map of hashtags
   * @param urls Item-count map of URL's
   */
  case class TweetNoText(
    user: String,
    min_timestamp: Long,
    max_timestamp: Long,
    geo_timestamp: Long,
    lat: Double,
    long: Double,
    followers: Int,
    following: Int,
    numtweets: Int,
    user_mentions: Map[String, Int],
    retweets: Map[String, Int],
    hashtags: Map[String, Int],
    urls: Map[String, Int]
    /* NOTE: If you add a field here, you need to update a bunch of places,
       including (of course) wherever a TweetNoText is created, but also
       some less obvious places.  In all:

       -- the doc string just above
       -- the definition of TweetNoText.empty()
       -- parse_json_lift() above
       -- merge_records() above
       -- nicely_format_plain() above and output_schema() below
    */
  )
  object TweetNoText {
    def empty =
      TweetNoText("", 0, 0, 0, Double.NaN, Double.NaN, 0, 0, 0,
        Map[String, Int](), Map[String, Int](),
        Map[String, Int](), Map[String, Int]())
  }
  implicit val tweetNoTextFmt =
    mkCaseWireFormat(TweetNoText.apply _, TweetNoText.unapply _)
  implicit val tweetFmt = mkCaseWireFormat(Tweet.apply _, Tweet.unapply _)

  // type TweetNoText = (String, Long, Double, Double, Int, Int, Int)
  // TweetNgram = Data for the tweet minus the text, plus an individual ngram
  //   from the text = (tweet_no_text_as_string, ngram)

  // type Tweet = (String, Long, String, Double, Double, Int, Int, Int)
  // TweetID = numeric string used to uniquely identify a tweet.
  type TweetID = String

  /**
   * A tweet along with ancillary data used for merging and filtering.
   *
   * @param key Key used for grouping (username or timestamp); stores the
   *   raw text of a JSON when `--output-format=raw`.
   * @param matches Whether the tweet matches the user-level boolean filters
   *   (if any)
   * @param tweet The tweet itself.
   */
  case class Record(
    key: String,
    matches: Boolean,
    tweet: Tweet
  )
  implicit val recordWire = mkCaseWireFormat(Record.apply _, Record.unapply _)

  object Record {
    def empty = Record("", true, Tweet.empty)
  }

  // IDRecord = Tweet ID along with all other data for a tweet.
  type IDRecord = (TweetID, Record)
  type TweetNgram = (String, String)
  // NgramCount = (ngram, number of ocurrences)
  type NgramCount = (String, Long)

  abstract class GroupTwitterPullAction extends ScoobiProcessFilesShared {
    val progname = "GroupTwitterPull"

    def create_parser(expr: String, foldcase: Boolean) = {
      if (expr == null) null
      else new TweetFilterParser(foldcase).parse(expr)
    }
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
      def matches(tweet: Tweet): Boolean = {
        // FIXME: When a filter is present, we may end up calling Twokenize
        // 2 or 3 times (once when generating words or n-grams, once when
        // implementing tweet-level filters, and once when implementing
        // user-level filters).  But the alternative is to pass around the
        // tokenized text, which might not be any faster in a Hadoop env.
        val tokenized = tweet.text flatMap (Twokenize(_))
        if (foldcase)
          matches(tweet, tokenized map (_.toLowerCase))
        else
          matches(tweet, tokenized)
      }

      // Not meant to be called externally.  Actually implement the matching,
      // with the text explicitly given (so it can be downcased to implement
      // case-insensitive matching).
      def matches(tweet: Tweet, text: Seq[String]): Boolean
    }

    case class EConst(value: Seq[String]) extends Expr {
      def matches(tweet: Tweet, text: Seq[String]) = text containsSlice value
    }

    case class EAnd(left:Expr, right:Expr) extends Expr {
      def matches(tweet: Tweet, text: Seq[String]) =
        left.matches(tweet, text) && right.matches(tweet, text)
    }

    case class EOr(left:Expr, right:Expr) extends Expr {
      def matches(tweet: Tweet, text: Seq[String]) =
        left.matches(tweet, text) || right.matches(tweet, text)
    }

    case class ENot(e:Expr) extends Expr {
      def matches(tweet: Tweet, text: Seq[String]) =
        !e.matches(tweet, text)
    }

    def time_compare(time1: Long, op: String, time2: Long) = {
      op match {
        case "<" => time1 < time2
        case "<=" => time1 <= time2
        case ">" => time1 > time2
        case ">=" => time1 >= time2
      }
    }

    def time_compare(tweet: Tweet, op: String, time: Long): Boolean = {
      val tn = tweet.notext
      assert(tn.min_timestamp == tn.max_timestamp)
      time_compare(tn.min_timestamp, op, time)
    }

    case class TimeCompare(op: String, time: Long) extends Expr {
      def matches(tweet: Tweet, text: Seq[String]) =
        time_compare(tweet, op, time)
    }

    case class TimeWithin(interval: (Long, Long)) extends Expr {
      def matches(tweet: Tweet, text: Seq[String]) = {
        val (start, end) = interval
        time_compare(tweet, ">=", start) &&
        time_compare(tweet, "<", end)
      }
    }

    // NOT CURRENTLY USED, but potentially useful as an indicator of how to
    // implement a parser for numbers.
//    class ExprLexical extends StdLexical {
//      override def token: Parser[Token] = floatingToken | super.token
//
//      def floatingToken: Parser[Token] =
//        rep1(digit) ~ optFraction ~ optExponent ^^
//          { case intPart ~ frac ~ exp => NumericLit(
//              (intPart mkString "") :: frac :: exp :: Nil mkString "")}
//
//      def chr(c:Char) = elem("", ch => ch==c )
//      def sign = chr('+') | chr('-')
//      def optSign = opt(sign) ^^ {
//        case None => ""
//        case Some(sign) => sign
//      }
//      def fraction = '.' ~ rep(digit) ^^ {
//        case dot ~ ff => dot :: (ff mkString "") :: Nil mkString ""
//      }
//      def optFraction = opt(fraction) ^^ {
//        case None => ""
//        case Some(fraction) => fraction
//      }
//      def exponent = (chr('e') | chr('E')) ~ optSign ~ rep1(digit) ^^ {
//        case e ~ optSign ~ exp =>
//          e :: optSign :: (exp mkString "") :: Nil mkString ""
//      }
//      def optExponent = opt(exponent) ^^ {
//        case None => ""
//        case Some(exponent) => exponent
//      }
//    }

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
    lexical.reserved ++= List("AND", "OR", "NOT", "TIME", "WITHIN")
    lexical.delimiters ++= List("(", ")", "<", "<=", ">", ">=")

    def word = stringLit ^^ {
      s => EConst(Seq(if (foldcase) s.toLowerCase else s))
    }

    def words = word.+ ^^ {
      x => EConst(x.flatMap(_ match { case EConst(y) => y }))
    }

    def compare_op = ( "<=" | "<" | ">=" | ">" )

    def time = stringLit ^^ { s => (s, parse_date(s)) } ^? (
      { case (_, Some(x)) => x },
      { case (s, None) => "Unable to parse date %s" format s } )

    def short_interval = stringLit ^^ { parse_date_interval(_) } ^? (
      { case (Some((from, to)), "") => (from, to) },
      { case (None, errmess) => errmess } )
     
    def full_interval = "(" ~> time ~ time <~ ")" ^^ {
      case from ~ to => (from, to) }
    
    def interval = (short_interval | full_interval)

    def time_compare = "TIME" ~> compare_op ~ time ^^ {
      case op ~ time => TimeCompare(op, time)
      // case op~time if parse_time(time) => TimeCompare(op, time)
    }

    def time_within = "TIME" ~> "WITHIN" ~> interval ^^ {
      interval => TimeWithin(interval)
    }

    def parens: Parser[Expr] = "(" ~> expr <~ ")"

    def not: Parser[ENot] = "NOT" ~> term ^^ { ENot(_) }

    def term = ( words | parens | not | time_compare | time_within )

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
          throw new
            IllegalArgumentException("Bad syntax: %s: %s" format (s, e))
      }
    }

    def test(exprstr: String, tweet: Tweet) = {
      maybe_parse(exprstr) match {
        case Success(tree, _) =>
          println("Tree: "+tree)
          val v = tree.matches(tweet)
          println("Eval: "+v)
        case e: NoSuccess => errprint("%s\n" format e)
      }
    }

    //A main method for testing
    def main(args: Array[String]) = {
      val text = args(2)
      val timestamp = parse_date(args(1)) match {
        case Some(time) => time
        case None => throw new IllegalArgumentException(
          "Unable to parse date %s" format args(1))
      }
      val tweet =
        Tweet(Seq(text), TweetNoText("user", timestamp, timestamp,
          timestamp, Double.NaN, Double.NaN, 0, 0, 1,
          Map[String, Int](), Map[String, Int](),
          Map[String, Int](), Map[String, Int]()))
      test(args(0), tweet)
    }
  }

  class ParseAndUniquifyTweets(opts: GroupTwitterPullParams)
      extends GroupTwitterPullAction {

    val operation_category = "Parse"

    /**
     * An empty tweet, stored as a full IDRecord.
     */
    val empty_tweet: IDRecord = ("", Record.empty)

    // Used internally to force an exit when a problem in parse_json_lift
    // occurs.
    private class ParseJSonExit extends Exception { }

    /**
     * Parse a JSON line into a tweet, using Lift.
     *
     * @return status and record.
     * Normally, when status == "success", record consists of the following:
     * (tweet_id, (key, tweet)) where `tweet_id` is the ID of the tweet,
     * `key` is what the grouping is done on, and `tweet` is the extracted
     * data of the tweet (a `Tweet` object).  However, when --output-format=raw,
     * we want to output the original JSON directly, so instead we return
     * (tweet_id, (line, Tweet.empty)) where `line` is the original JSON
     * text and `Tweet.empty` is a fixed, empty Tweet.  We still need the
     * `tweet_id` so that we can group on it to eliminate duplicates.
     */
    def parse_json_lift(line: String): (String, IDRecord) = {

      /**
       * Convert a Twitter timestamp, e.g. "Tue Jun 05 14:31:21 +0000 2012",
       * into a time in milliseconds since the Epoch (Jan 1 1970, or so).
       */
      def parse_time(timestring: String): Long = {
        val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
        try {
          sdf.parse(timestring)
          sdf.getCalendar.getTimeInMillis
        } catch {
          case pe: ParseException => {
            bump_counter("unparsable date")
            logger.warn("Error parsing date %s on line %s: %s\n%s" format (
              timestring, lineno, line, pe))
            0
          }
        }
      }

      def parse_problem(e: Exception) = {
        logger.warn("Error parsing line %s: %s\n%s" format (
          lineno, line, stack_trace_as_string(e)))
        ("error", empty_tweet)
      }

      /**
       * Retrieve a string along a path, checking to make sure the path
       * exists.
       */
      def force_string(value: liftweb.json.JValue, fields: String*) = {
        var fieldval = value
        var path = List[String]()
        for (field <- fields) {
          path :+= field
          fieldval \= field
          if (fieldval == liftweb.json.JNothing) {
            val fieldpath = path mkString "."
            bump_counter("ERROR: tweet with missing field %s" format fieldpath)
            warning(line, "Can't find field path %s in tweet", fieldpath)
            throw new ParseJSonExit
          }
        }
        fieldval.values.toString
      }

      /**
       * Retrieve the list of entities of a particular type from a tweet,
       * along with the indices referring to the entity.
       *
       * @param key the key referring to the type of entity
       * @param subkey the subkey within the entity to return as the "value"
       *   of the entity
       */
      def retrieve_entities_with_indices(parsed: liftweb.json.JValue,
          key: String, subkey: String) = {
        // Retrieve the raw list of entities based on the key.
        val entities_raw =
          (parsed \ "entities" \ key values).
          asInstanceOf[List[Map[String, Any]]]
        // For each entity, fetch the user actually mentioned. Sort and
        // convert into a map counting mentions.  Do it this way to count
        // multiple mentions properly.
        for { ent <- entities_raw
              rawvalue = ent(subkey)
              if rawvalue != null
              value = rawvalue.toString
              indices = ent("indices").asInstanceOf[List[Number]]
              start = indices(0).intValue
              end = indices(1).intValue
              if {
                if (value.length == 0) {
                  bump_counter("zero length %s/%s seen" format (key, subkey))
                  warning(line,
                    "Zero-length %s/%s in interval [%d,%d], skipped",
                    key, subkey, start, end)
                  false
                } else true
              }
            }
          yield (value, start, end)
      }

      /**
       * Retrieve the list of entities of a particular type from a tweet,
       * as a map with counts (so as to count multiple identical entities
       * properly).
       *
       * @param key the key referring to the type of entity
       * @param subkey the subkey within the entity to return as the "value"
       *   of the entity
       */
      def retrieve_entities(parsed: liftweb.json.JValue,
          key: String, subkey: String) = {
        list_to_item_count_map(
          for { (value, start, end) <-
                retrieve_entities_with_indices(parsed, key, subkey) }
            yield value
        )
      }

      try {
        /* The result of parsing is a JValue, which is an abstract type with
           subtypes for all of the types of objects found in JSON: maps, arrays,
           strings, integers, doubles, booleans.  Corresponding to the atomic
           types are JString, JInt, JDouble, JBool.  Corresponding to arrays is
           JArray (which underlyingly holds something like a List[JValue]), and
           corresponding to maps is JObject.  JObject underlyingly holds
           something like a List[JField], and a JField is a structure holding a
           'name' (a string) and a 'value' (a JValue).  The following
           operations can be done on these objects:

           1. For JArray, JObject and JField, the method 'children' yields
              a List[JValue] of their children.  For JObject, as mentioned
              above, this will be a list of JField objects.  For JField
              objects, this will be a one-element list of whatever was
              in 'value'.
           2. JArray, JObject and JField can be indexed like an array.
              Indexing JArray is obvious.  Indexing JObject gives a JField
              object, and indexing JField tries to index the object in the
              field's 'value' element.
           3. You can directly fetch the components of a JField using the
              accessors 'name' and 'value'.

           4. You can retrieve the underlying Scala form of any object using
              'values'.  This returns JArrays as a List, JObject as a Map, and
              JField as a tuple of (name, value).  The conversion is deep, in
              that subobjects are recursively converted.  However, the result
              type isn't necessarily useful -- for atoms you get more or less
              what you expect (although BigInt instead of Int), and for JField
              it's just a tuple, but for JArray and JObject the type of the
              expression is a path-dependent type ending in 'Value'.  So you
              will probably have to cast it using asInstanceOf[]. (For Maps,
              consider using Map[String, Any], since you don't necessarily know
              the type of the different elements, which may vary from element
              to element.

           5. For JObject, you can do a field lookup using the "\" operator,
              as shown below.  This yields a JValue, onto which you can do
              another "\" if it happens to be another JObject. (That's because
              "\", like 'apply', 'values' and 'children' is defined on the
              JValue itself.  When "\" is given a non-existent field name,
              or run on a JInt or other atom, the result is JNothing.
           6. You can also pass a class (one of the subclasses of JValue) to
              "\" instead of a string, e.g. classOf[JInt], to find children
              with this type.  BEWARE: It looks only at the children returned
              by the 'children' method, which (for objects) are all of type
              JField, so looking for JInt won't help even if you have some
              key-value pairs where the value is an integer.
           7. You can also use "\\" to do multi-level lookup, i.e. this
              recursively traipses down 'children' and 'children' of 'children'
              and does the equivalent of "\" on each.  The return value is a
              List, with 'values' applied to each element of the List to
              convert to Scala objects.  Beware that you only get the items
              themselves, without the field names. That is, if you search for
              e.g. a JInt, you'll get back a List[Int] with a lot of numbers
              in it -- but no indication of where they came from or what the
              associated field name was.
           8. There's also 'extract' for getting a JObject as a case class,
              as well as 'map', 'filter', '++', and other standard methods for
              collections.
        */
        val parsed = liftweb.json.parse(line)
        if ((parsed \ "delete" values) != None) {
          bump_counter("tweet deletion notices skipped")
          ("delete", empty_tweet)
        } else if ((parsed \ "limit" values) != None) {
          bump_counter("tweet limit notices skipped")
          ("limit", empty_tweet)
        } else {
          val user = force_string(parsed, "user", "screen_name")
          val timestamp = parse_time(force_string(parsed, "created_at"))
          val raw_text = force_string(parsed, "text")
          val text = raw_text.replaceAll("\\s+", " ")
          val followers = force_string(parsed, "user", "followers_count").toInt
          val following = force_string(parsed, "user", "friends_count").toInt
          val tweet_id = force_string(parsed, "id_str")
          val (lat, long) =
            if ((parsed \ "coordinates" \ "type" values).toString != "Point") {
              (Double.NaN, Double.NaN)
            } else {
              val latlong: List[Number] =
                (parsed \ "coordinates" \ "coordinates" values).
                  asInstanceOf[List[Number]]
              (latlong(1).doubleValue, latlong(0).doubleValue)
            }

          /////////////// HANDLE ENTITIES

          /* Entity types:

           user_mentions: @-mentions in the text; subkeys are
             screen_name = Username of user
             name = Display name of user
             id_str = ID of user, as a string
             id = ID of user, as a number
             indices = indices in text of @-mention, including the @

           urls: URLs mentioned in the text; subkeys are
             url = raw URL (probably a shortened reference to bit.ly etc.)
             expanded_url = actual URL
             display_url = display form of URL (without initial http://, cut
               off after a point with \u2026 (Unicode ...)
             indices = indices in text of raw URL
             (NOTE: all URL's in the JSON text have /'s quoted as \/, and
              display_url may not be present)
           
           hashtags: Hashtags mentioned in the text; subkeys are
             text = text of the hashtag
             indices = indices in text of hashtag, including the #

           media: Embedded objects
             type = "photo" for images
             indices = indices of URL for image
             url, expanded_url, display_url = similar to URL mentions
             media_url = photo on p.twimg.com?
             media_url_https = https:// alias for media_url
             id_str, id = some sort of tweet ID or similar?
             sizes = map with keys "small", "medium", "large", "thumb";
               each has subkeys:
                 resize = "crop" or "fit"
                 h = height (as number)
                 w = width (as number)
             
             Example of URL's for photo:
  url = http:\/\/t.co\/AO3mRYaG
  expanded_url = http:\/\/twitter.com\/alejandraoraa\/status\/215758169589284864\/photo\/1
  display_url = pic.twitter.com\/AO3mRYaG
  media_url = http:\/\/p.twimg.com\/Av6G6YBCAAAwD7J.jpg
  media_url_https = https:\/\/p.twimg.com\/Av6G6YBCAAAwD7J.jpg

"id_str":"215758169597673472"
           */

          // Retrieve "user_mentions", which is a list of mentions, each
          // listing the span of text, the user actually mentioned, etc. --
          // along with whether the mention is a retweet (by looking for
          // "RT" before the mention)
          val user_mentions_raw = retrieve_entities_with_indices(
            parsed, "user_mentions", "screen_name")
          val user_mentions_retweets =
            for { (screen_name, start, end) <- user_mentions_raw
                  namelen = screen_name.length
                  retweet = (start >= 3 &&
                             raw_text.slice(start - 3, start) == "RT ")
                } yield {
              // Subtract one because of the initial @ in the index reference
              if (end - start - 1 != namelen) {
                bump_counter("wrong length interval for screen name seen")
                warning(line, "Strange indices [%d,%d] for screen name %s, length %d != %d, text context is '%s'",
                  start, end, screen_name, end - start - 1, namelen,
                  raw_text.slice(start, end))
              }
              (screen_name, retweet)
            }
          val user_mentions_list =
            for { (screen_name, retweet) <- user_mentions_retweets }
              yield screen_name
          val retweets_list =
            for { (screen_name, retweet) <- user_mentions_retweets if retweet }
              yield screen_name
          val user_mentions = list_to_item_count_map(user_mentions_list)
          val retweets = list_to_item_count_map(retweets_list)

          val hashtags = retrieve_entities(parsed, "hashtags", "text")
          val urls = retrieve_entities(parsed, "urls", "expanded_url")
          // map
          //  { case (url, count) => (url.replace("\\/", "/"), count) }

          val key =
            if (opts.output_format == "raw") line
            else opts.grouping match {
              case "user" => user
              case "time" =>
                ((timestamp / opts.timeslice) * opts.timeslice).toString
              case "none" => tweet_id
            }
          ("success",
            (tweet_id, Record(key, true,
              Tweet(Seq(text), TweetNoText(user, timestamp, timestamp,
                timestamp, lat, long, followers, following, 1, user_mentions,
                retweets, hashtags, urls)))))
        }
      } catch {
        case jpe: liftweb.json.JsonParser.ParseException => {
          bump_counter("ERROR: lift-json parsing error")
          parse_problem(jpe)
        }
        case npe: NullPointerException => {
          bump_counter("ERROR: NullPointerException when parsing")
          parse_problem(npe)
        }
        case nfe: NumberFormatException => {
          bump_counter("ERROR: NumberFormatException when parsing")
          parse_problem(nfe)
        }
        case _: ParseJSonExit => ("error", empty_tweet)
        case e: Exception => {
          bump_counter("ERROR: %s when parsing" format e.getClass.getName)
          parse_problem(e); throw e
        }
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

    /*
     * Parse a JSON line into a tweet.  Return value is an IDRecord, including
     * the tweet ID, username, text and all other data.
     */
    def parse_json(line: String): IDRecord = {
      bump_counter("total lines")
      lineno += 1
      // For testing
      // logger.debug("parsing JSON: %s" format line)
      if (line.trim == "") {
        bump_counter("blank lines skipped")
        empty_tweet
      }
      else {
        bump_counter("total tweets parsed")
        val (status, record) = parse_json_lift(line)
        if (status == "error") {
          bump_counter("total tweets unsuccessfully parsed")
        } else if (status == "success") {
          bump_counter("total tweets successfully parsed")
        } else {
          bump_counter("total tweets skipped")
        }
        record
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
      val (tw_id, r) = id_r
      val tn = r.tweet.notext
      tw_id != "" && tn.min_timestamp != 0 && tn.max_timestamp != 0 &&
        tn.user != "" && !(tn.lat == 0.0 && tn.long == 0.0)
    }

    /**
     * Select the first tweet with the same ID.  For various reasons we may
     * have duplicates of the same tweet among our data.  E.g. it seems that
     * Twitter itself sometimes streams duplicates through its Streaming API,
     * and data from different sources will almost certainly have duplicates.
     * Furthermore, sometimes we want to get all the tweets even in the
     * presence of flakiness that causes Twitter to sometimes bomb out in a
     * Streaming session and take a while to restart, so we have two or three
     * simultaneous streams going recording the same stuff, hoping that Twitter
     * bombs out at different points in the different sessions (which is
     * generally true). Then, all or almost all the tweets are available in
     * the different streams, but there is a lot of duplication that needs to
     * be tossed aside.
     */
    def tweet_once(id_rs: (TweetID, Iterable[Record])): Record = {
      val (id, rs) = id_rs
      rs.head
    }

    lazy val filter_tweets_ast =
      create_parser(opts.filter_tweets, foldcase = true)
    lazy val cfilter_tweets_ast =
      create_parser(opts.cfilter_tweets, foldcase = false)

    /**
     * Apply any boolean filters given in `--filter-tweets` or
     * `--cfilter-tweets`.
     */
    def filter_tweet_by_tweet_filters(tweet: Tweet) = {
      (filter_tweets_ast == null || (filter_tweets_ast matches tweet)) &&
       (cfilter_tweets_ast == null || (cfilter_tweets_ast matches tweet))
    }

    def apply(lines: DList[String]) = {

      // Filter out duplicate tweets -- group by tweet ID and then take the
      // first tweet for a given ID.  Duplicate tweets occur for various
      // reasons, e.g. sometimes in the stream itself due to Twitter errors,
      // or when combining data from multiple, possibly overlapping, sources.
      // In the process, the tweet ID grouping key is discarded (it's also
      // inside the tweet data).
      def filter_duplicates(values: DList[IDRecord]): DList[Record] =
        values.groupByKey.map(tweet_once)

      def filter_tweets(values: DList[IDRecord]) =
        filter_duplicates(values).filter(x =>
          filter_tweet_by_tweet_filters(x.tweet))

      // Parse JSON into tweet records (IDRecord).
      val values_extracted = lines.map(parse_json)

      /* Filter duplicates, invalid tweets, tweets not matching any
         tweet-level boolean filters. (User-level boolean filters get
         applied later.) */
      filter_tweets(values_extracted.filter(is_valid_tweet))
    }
  }

  class GroupTweetsAndSelectGood(opts: GroupTwitterPullParams)
      extends GroupTwitterPullAction {

    val operation_category = "Group"

    /**
     * Merge the data associated with two tweets or tweet combinations
     * into a single tweet combination.  Concatenate text.  Find maximum
     * numbers of followers and followees.  Add number of tweets in each.
     * For latitude and longitude, take the earliest provided values
     * ("earliest" by timestamp and "provided" meaning not missing).
     */
    def merge_records(tw1: Record, tw2: Record): Record = {
      assert(tw1.key == tw2.key)
      val t1 = tw1.tweet.notext
      val t2 = tw2.tweet.notext
      val (followers, following) =
        (math.max(t1.followers, t2.followers),
         math.max(t1.following, t2.following))
      val text = tw1.tweet.text ++ tw2.tweet.text
      val numtweets = t1.numtweets + t2.numtweets
      val user_mentions = combine_maps(t1.user_mentions, t2.user_mentions)
      val retweets = combine_maps(t1.retweets, t2.retweets)
      val hashtags = combine_maps(t1.hashtags, t2.hashtags)
      val urls = combine_maps(t1.urls, t2.urls)

      val (lat, long, geo_timestamp) =
        if (isNaN(t1.lat) && isNaN(t2.lat)) {
          (t1.lat, t1.long, math.min(t1.geo_timestamp, t2.geo_timestamp))
        } else if (isNaN(t2.lat)) {
          (t1.lat, t1.long, t1.geo_timestamp)
        } else if (isNaN(t1.lat)) {
          (t2.lat, t2.long, t2.geo_timestamp)
        } else if (t1.geo_timestamp < t2.geo_timestamp) {
          (t1.lat, t1.long, t1.geo_timestamp)
        } else {
          (t2.lat, t2.long, t2.geo_timestamp)
        }
      val min_timestamp = math.min(t1.min_timestamp, t2.min_timestamp)
      val max_timestamp = math.max(t1.max_timestamp, t2.max_timestamp)

      // FIXME maybe want to track the different users
      val tweet =
        Tweet(text, TweetNoText(t1.user, min_timestamp, max_timestamp,
          geo_timestamp, lat, long, followers, following, numtweets,
          user_mentions, retweets, hashtags, urls))
      Record(tw1.key, tw1.matches || tw2.matches, tweet)
    }

    /**
     * Return true if tweet (combination) has a fully-specified latitude
     * and longitude.
     */
    def has_latlong(r: Record) = {
      val tw = r.tweet
      !isNaN(tw.notext.lat) && !isNaN(tw.notext.long)
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
      val tn = r.tweet.notext

      val retval =
        (tn.following >= MIN_NUMBER_FOLLOWING &&
           tn.following <= MAX_NUMBER_FOLLOWING) &&
        (tn.followers >= MIN_NUMBER_FOLLOWERS) &&
        (tn.numtweets >= opts.min_tweets && tn.numtweets <= opts.max_tweets)
      if (opts.debug && retval == false)
        logger.info("Rejecting is_nonspammer %s" format r)
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
      val tn = r.tweet.notext
      val retval = (tn.lat >= MIN_LAT && tn.lat <= MAX_LAT) &&
                   (tn.long >= MIN_LNG && tn.long <= MAX_LNG)
      if (opts.debug && retval == false)
        logger.info("Rejecting northamerica_only %s" format r)
      retval
    }

    def is_good_geo_tweet(r: Record): Boolean = {
      if (opts.debug)
        logger.info("Considering %s" format r)
      has_latlong(r) &&
      is_nonspammer(r) &&
      northamerica_only(r)
    }

    lazy val filter_users_ast =
      create_parser(opts.filter_users, foldcase = true)
    lazy val cfilter_users_ast =
      create_parser(opts.cfilter_users, foldcase = false)

    /**
     * Apply any boolean filters given in `--filter-users` or
     * `--cfilter-users`.
     */
    def filter_tweet_by_user_filters(tweet: Tweet) = {
      (filter_users_ast == null || (filter_users_ast matches tweet)) &&
       (cfilter_users_ast == null || (cfilter_users_ast matches tweet))
    }

    def apply(tweets: DList[Record]) = {
      // Group by username, then combine the tweets for a user into a
      // tweet combination, with text concatenated and the location taken
      // from the earliest tweet with a specific coordinate.
      val concatted = tweets.groupBy(_.key).combine(merge_records).map(_._2)

      // If grouping by user, filter the tweet combinations, removing users
      // without a specific coordinate; users that appear to be "spammers" or
      // other users with non-standard behavior; and users located outside of
      // North America.  FIXME: We still want to filter spammers; but this
      // is trickier when not grouping by user.  How to do it?
      val good_tweets =
        if (opts.geographic_only) concatted.filter(is_good_geo_tweet)
        else concatted

      good_tweets.filter(_.matches)
    }
  }

  class TokenizeFilterAndCountTweets(opts: GroupTwitterPullParams)
      extends GroupTwitterPullAction {

    val operation_category = "Tokenize"

    /**
     * Convert a word to lowercase.
     */
    def normalize_word(orig_word: String) = {
      val word =
        if (opts.preserve_case)
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

    /**
     * Use Twokenize to break up a tweet into tokens and separate into ngrams.
     */
    def break_tweet_into_ngrams(text: String):
        Iterable[Iterable[String]] = {
      val words = Twokenize(text)
      val normwords = words.map(normalize_word)

      // Then, generate all possible ngrams up to a specified maximum length,
      // where each ngram is a sequence of words.  `sliding` overlays a sliding
      // window of a given size on a sequence to generate successive
      // subsequences -- exactly what we want to generate ngrams.  So we
      // generate all 1-grams, then all 2-grams, etc. up to the maximum size,
      // and then concatenate the separate lists together (that's what `flatMap`
      // does).
      (1 to opts.max_ngram).
        flatMap(normwords.sliding(_)).filter(!reject_ngram(_))
    }

    /**
     * Tokenize a tweet text string into ngrams and count them, emitting
     * the word-count pairs encoded into a string.
     * and emit the ngrams individually.
     * Each ngram is emitted along with the text data and a count of 1,
     * and later grouping + combining will add all the 1's to get the
     * ngram count.
     */
    def emit_ngrams(tweet_text: Seq[String]): String = {
      val ngrams =
        tweet_text.flatMap(break_tweet_into_ngrams(_)).toSeq.
          map(encode_ngram_for_counts_field)
      shallow_encode_word_count_map(list_to_item_count_map(ngrams).toSeq)
    }

    /**
     * Given a tweet, tokenize the text into ngrams, count words and format
     * the result as a field; then convert the whole into a record to be
     * written out.
     */
    def tokenize_count_and_format(record: Record): String = {
      val tweet = record.tweet
      val tnt = tweet.notext
      val formatted_text = emit_ngrams(tweet.text)
      // Latitude/longitude need to be combined into a single field, but only
      // if both actually exist.
      val latlongstr =
        if (!isNaN(tnt.lat) && !isNaN(tnt.long))
          "%s,%s" format (tnt.lat, tnt.long)
        else ""
      val user_mentions = encode_word_count_map(tnt.user_mentions.toSeq)
      val retweets = encode_word_count_map(tnt.retweets.toSeq)
      val hashtags = encode_word_count_map(tnt.hashtags.toSeq)
      val urls = encode_word_count_map(tnt.urls.toSeq)
      // Put back together but drop key.
      Seq(tnt.user, tnt.min_timestamp, tnt.max_timestamp, tnt.geo_timestamp,
          latlongstr, tnt.followers, tnt.following, tnt.numtweets,
          user_mentions, retweets, hashtags, urls,
          tweet.text.map(encode_string_for_field(_)) mkString ">>",
          formatted_text
        ) mkString "\t"
    }
  }

  class GroupTwitterPull(opts: GroupTwitterPullParams)
      extends GroupTwitterPullAction {

    val operation_category = "Driver"

    def corpus_suffix = {
      val dist_type = if (opts.max_ngram == 1) "unigram" else "ngram"
      "%s-%s-counts" format (opts.split, dist_type)
    }

    /**
     * Output a schema file of the appropriate name.
     */
    def output_schema(filehand: FileHandler) {
      val filename = Schema.construct_schema_file(filehand,
        opts.output, opts.corpus_name, corpus_suffix)
      logger.info("Outputting a schema to %s ..." format filename)
      val fields = Seq("user", "min-timestamp", "max-timestamp",
        "geo-timestamp","coord", "followers", "following",
        "numtweets", "user-mentions", "retweets", "hashtags", "urls",
        "text", "counts")
      val fixed_fields = Map(
        "corpus" -> opts.corpus_name,
        "corpus-type" -> ("twitter-%s" format opts.grouping),
        "split" -> opts.split
      ) ++ (
        if (opts.grouping == "time")
          Map("corpus-timeslice" -> opts.timeslice.toString)
        else
          Map[String, String]()
      )
      val schema = new Schema(fields, fixed_fields)
      schema.output_schema_file(filehand, filename)
    }
  }

  def create_params(ap: ArgParser) = new GroupTwitterPullParams(ap)
  val progname = "GroupTwitterPull"

  def run() {
    val opts = init_scoobi_app()
    val filehand = new HadoopFileHandler(configuration)
    if (opts.corpus_name == null) {
      val (_, last_component) = filehand.split_filename(opts.input)
      opts.corpus_name = last_component.replace("*", "_")
    }
    errprint("GroupTwitterPull: " + (opts.grouping match {
      case "time" =>
        "grouping by time, with slices of %g seconds".
          format(opts.timeslice_float)
      case "user" =>
        "grouping by user"
      case "none" if opts.output_format == "raw" =>
        "not grouping, outputting tweets as raw JSON"
      case "none" =>
        "not grouping"
    }))
    val ptp = new GroupTwitterPull(opts)

    // Firstly we load up all the (new-line-separated) JSON lines.
    val lines: DList[String] = TextInput.fromTextFile(opts.input)

    val tweets1 = new ParseAndUniquifyTweets(opts)(lines)

    /* If we're outputting raw, output the JSON that we stashed into
       the grouping key (the `key` part of the `Record` class). */
    if (opts.output_format == "raw") {
      persist(TextOutput.toTextFile(tweets1.map(_.key), opts.output))
    } else {
      val grouped_tweets = new GroupTweetsAndSelectGood(opts)(tweets1)
      val tfct = new TokenizeFilterAndCountTweets(opts)
      // Tokenize the combined text into words, possibly generate ngrams
      // from them, count them up and output results formatted into a record.
      val nicely_formatted = grouped_tweets.map(tfct.tokenize_count_and_format)
      persist(TextOutput.toTextFile(nicely_formatted, opts.output))
    }
    errprint("GroupTwitterPull: done.")

    rename_output_files(configuration.fs, opts.output, opts.corpus_name,
      ptp.corpus_suffix)

    // create a schema
    if (opts.output_format != "raw")
      ptp.output_schema(filehand)

    finish_scoobi_app(opts)
  }
  /*

  To build a classifier for conserv vs liberal:

  1. Look for people retweeting congressmen or governor tweets, possibly at
     some minimum level of retweeting (or rely on followers, for some
     minimum number of people following)
  2. Make sure either they predominate having retweets from one party,
     and/or use the DW-NOMINATE scores to pick out people whose average
     ideology score of their retweets is near the extremes.
  */
}

