//  ParseTweets.scala
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
import java.util.Date

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
import tgutil.textutil.with_commas
import tgutil.timeutil._

class ParseTweetsParams(ap: ArgParser) extends
    ScoobiProcessFilesParams(ap) {
  var grouping = ap.option[String]("grouping", "g", "gr", "group",
    choices = Seq("user", "time", "none"),
    help="""Mode for grouping tweets.  There are currently three methods
    of grouping: `user`, `time` (i.e. all tweets within a given
    timeslice, specified with `--timeslice`) and `none` (no grouping;
    tweets are passed through directly, after duplicated tweets have been
    removed).  Default is `user` when `--ouput-format=corpus`, and `none`
    otherwise.""")
  var output_format = ap.option[String]("output-format", "of",
    choices = Seq("corpus", "stats", "raw"),
    default = "corpus",
    help="""Format for output of tweets or tweet groups.  Possibilities are
    
    -- `corpus` (Store in a TextGrounder-style corpus, i.e. as a simple database
    with one record per line, fields separated by TAB characters, and a
    schema indicating the names of the columns.)
  
    -- `raw` (Simply output JSON-formatted tweets directly, exactly as received;
    only possible for `--grouping=none`, in which case the input tweets will be
    output directly, after removing duplicates.)
    
    -- `stats` (Corpus-style output with statistics on the tweets, users, etc.
    rather than the tweets themselves.  Generally doesn't make much sense when
    used with any kind of grouping, because it will then output statistics on
    the results of grouping rather than on the actual tweets.)""")
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
  var filter_groups = ap.option[String]("filter-groups",
    help="""Boolean expression used to filter on the grouped-tweet level.
  This is like `--filter-tweets` but filters groups of tweets (grouped
  according to `--grouping`), such that groups of tweets will be accepted
  if *any* tweet matches the filter.""")
  var cfilter_groups = ap.option[String]("cfilter-groups",
    help="""Same as `--filter-groups` but does case-sensitive matching.""")
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
    if (grouping == null)
      grouping = if (output_format == "corpus") "user" else "none"
    if (output_format == "raw" && grouping != "none")
      ap.usageError("`raw` output format only allowed when `--grouping=none`")
  }
}

object ParseTweets extends ScoobiProcessFilesApp[ParseTweetsParams] {
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

  // TweetID = Twitter's numeric ID used to uniquely identify a tweet.
  type TweetID = Long

  type Timestamp = Long

  /**
   * Data for a tweet or grouping of tweets.
   *
   * @param line Raw JSON for tweet; only stored when --output-format=raw
   * @param text Text for tweet or tweets (a Seq in case of multiple tweets)
   * @param user User name (FIXME: or one of them, when going by time; should
   *    do something smarter)
   * @param id Tweet ID
   * @param min_timestamp Earliest timestamp
   * @param max_timestamp Latest timestamp
   * @param geo_timestamp Earliest timestamp of tweet with corresponding
   *    location
   * @param lat Best latitude (corresponding to the earliest tweet)
   * @param long Best longitude (corresponding to the earliest tweet)
   * @param followers Max followers
   * @param following Max following
   * @param lang Language used
   * @param numtweets Number of tweets merged
   * @param user_mentions Item-count map of all @-mentions
   * @param retweets Like `user_mentions` but only for retweet mentions
   * @param hashtags Item-count map of hashtags
   * @param urls Item-count map of URL's
   */
  case class Tweet(
    json: String,
    text: Seq[String],
    user: String,
    id: TweetID,
    min_timestamp: Timestamp,
    max_timestamp: Timestamp,
    geo_timestamp: Timestamp,
    lat: Double,
    long: Double,
    followers: Int,
    following: Int,
    lang: String,
    numtweets: Int,
    user_mentions: Map[String, Int],
    retweets: Map[String, Int],
    hashtags: Map[String, Int],
    urls: Map[String, Int]
    /* NOTE: If you add a field here, you need to update a bunch of places,
       including (of course) wherever a Tweet is created, but also
       some less obvious places.  In all:

       -- the doc string just above
       -- the definition of to_row() and Tweet.row_fields()
       -- parse_json_lift() below
       -- merge_records() below
       -- TweetFilterParser.main() below
    */
  ) {
    def to_row = {
      import Encoder.{long => elong, _}
      // Latitude/longitude need to be combined into a single field, but only
      // if both actually exist.
      val latlongstr =
        if (!isNaN(lat) && !isNaN(long)) "%s,%s" format (lat, long)
        else ""
      // Drop key.
      Seq(
        string(user),
        elong(id),
        timestamp(min_timestamp),
        timestamp(max_timestamp),
        timestamp(geo_timestamp),
        latlongstr,
        int(followers),
        int(following),
        string(lang),
        int(numtweets),
        count_map(user_mentions),
        count_map(retweets),
        count_map(hashtags),
        count_map(urls),
        seq_string(text)
      ) mkString "\t"
    }
  }

  object Tweet {
    def row_fields =
      Seq("user", "id", "min-timestamp", "max-timestamp",
        "geo-timestamp","coord", "followers", "following", "lang",
        "numtweets", "user-mentions", "retweets", "hashtags", "urls",
        "text")
  }
  implicit val tweetWire = mkCaseWireFormat(Tweet.apply _, Tweet.unapply _)

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

  /**
   * A generic action in the ParseTweets app.
   */
  abstract class ParseTweetsAction extends ScoobiProcessFilesAction {
    val progname = "ParseTweets"

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

    def time_compare(time1: Timestamp, op: String, time2: Timestamp) = {
      op match {
        case "<" => time1 < time2
        case "<=" => time1 <= time2
        case ">" => time1 > time2
        case ">=" => time1 >= time2
      }
    }

    def time_compare(tw: Tweet, op: String, time: Timestamp): Boolean = {
      assert(tw.min_timestamp == tw.max_timestamp)
      time_compare(tw.min_timestamp, op, time)
    }

    case class TimeCompare(op: String, time: Timestamp) extends Expr {
      def matches(tweet: Tweet, text: Seq[String]) =
        time_compare(tweet, op, time)
    }

    case class TimeWithin(interval: (Timestamp, Timestamp)) extends Expr {
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
        Tweet("", Seq(text), "user", 0, timestamp, timestamp,
          timestamp, Double.NaN, Double.NaN, 0, 0, "unknown", 1,
          Map[String, Int](), Map[String, Int](),
          Map[String, Int](), Map[String, Int]())
      test(args(0), tweet)
    }
  }

  class ParseAndUniquifyTweets(
      opts: ParseTweetsParams,
      set_counters: Boolean = true
    ) extends ParseTweetsAction {

    val operation_category = "Parse"

    def maybe_counter(counter: String) {
      if (set_counters)
        bump_counter(counter)
    }

    // Used internally to force an exit when a problem in parse_json_lift
    // occurs.
    private class ParseJSonExit extends Exception { }

    /**
     * Parse a JSON line into a tweet, using Lift.
     *
     * @return status and tweet.
     */
    def parse_json_lift(line: String): (String, Tweet) = {

      /**
       * Convert a Twitter timestamp, e.g. "Tue Jun 05 14:31:21 +0000 2012",
       * into a time in milliseconds since the Epoch (Jan 1 1970, or so).
       */
      def parse_time(timestring: String): Timestamp = {
        val sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy")
        try {
          sdf.parse(timestring)
          sdf.getCalendar.getTimeInMillis
        } catch {
          case pe: ParseException => {
            maybe_counter("unparsable date")
            logger.warn("Error parsing date %s on line %s: %s\n%s" format (
              timestring, lineno, line, pe))
            0
          }
        }
      }

      def parse_problem(e: Exception) = {
        logger.warn("Error parsing line %s: %s\n%s" format (
          lineno, line, stack_trace_as_string(e)))
        ("error", null)
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
            maybe_counter("ERROR: tweet with missing field %s" format fieldpath)
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
                  maybe_counter("zero length %s/%s seen" format (key, subkey))
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
          maybe_counter("tweet deletion notices skipped")
          ("delete", null)
        } else if ((parsed \ "limit" values) != None) {
          maybe_counter("tweet limit notices skipped")
          ("limit", null)
        } else {
          val user = force_string(parsed, "user", "screen_name")
          val timestamp = parse_time(force_string(parsed, "created_at"))
          val raw_text = force_string(parsed, "text")
          val text = raw_text.replaceAll("\\s+", " ")
          val followers = force_string(parsed, "user", "followers_count").toInt
          val following = force_string(parsed, "user", "friends_count").toInt
          val tweet_id = force_string(parsed, "id_str")
          val lang = force_string(parsed, "user", "lang")
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
                maybe_counter("wrong length interval for screen name seen")
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

          ("success",
            Tweet(if (opts.output_format == "raw") line else "",
              Seq(text), user, tweet_id.toLong, timestamp,
              timestamp, timestamp, lat, long, followers, following, lang, 1,
              user_mentions, retweets, hashtags, urls))
        }
      } catch {
        case jpe: liftweb.json.JsonParser.ParseException => {
          maybe_counter("ERROR: lift-json parsing error")
          parse_problem(jpe)
        }
        case npe: NullPointerException => {
          maybe_counter("ERROR: NullPointerException when parsing")
          parse_problem(npe)
        }
        case nfe: NumberFormatException => {
          maybe_counter("ERROR: NumberFormatException when parsing")
          parse_problem(nfe)
        }
        case _: ParseJSonExit => ("error", null)
        case e: Exception => {
          maybe_counter("ERROR: %s when parsing" format e.getClass.getName)
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
     * Parse a JSON line into a tweet.  Return `null` if unable to parse.
     */
    def parse_json(line: String) = {
      maybe_counter("total lines")
      lineno += 1
      // For testing
      // logger.debug("parsing JSON: %s" format line)
      if (line.trim == "") {
        maybe_counter("blank lines skipped")
        null
      }
      else {
        maybe_counter("total tweets parsed")
        val (status, tweet) = parse_json_lift(line)
        if (status == "error") {
          maybe_counter("total tweets unsuccessfully parsed")
        } else if (status == "success") {
          maybe_counter("total tweets successfully parsed")
        } else {
          maybe_counter("total tweets skipped")
        }
        tweet
      }
    }

    /**
     * Return true if this tweet is "valid" in that it doesn't have any
     * out-of-range values (blank strings or 0-valued quantities).  Note
     * that we treat a case where both latitude and longitude are 0 as
     * invalid even though technically such a place could exist. (FIXME,
     * use NaN or something to indicate a missing latitude or longitude).
     */
    def is_valid_tweet(tw: Tweet): Boolean = {
      // filters out invalid tweets, as well as trivial spam
      tw.id != 0 && tw.min_timestamp != 0 && tw.max_timestamp != 0 &&
        tw.user != "" && !(tw.lat == 0.0 && tw.long == 0.0)
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
    def tweet_once(id_tweets: (TweetID, Iterable[Tweet])) = {
      val (id, tweets) = id_tweets
      tweets.head
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

    // Filter out duplicate tweets -- group by tweet ID and then take the
    // first tweet for a given ID.  Duplicate tweets occur for various
    // reasons, e.g. sometimes in the stream itself due to Twitter errors,
    // or when combining data from multiple, possibly overlapping, sources.
    def filter_duplicates(values: DList[Tweet]) =
      values.groupBy(_.id).map(tweet_once)

    def filter_tweets(values: DList[Tweet]) =
      filter_duplicates(values).filter(x => filter_tweet_by_tweet_filters(x))

    /**
     * Parse a set of JSON-formatted tweets.
     */
    def apply(lines: DList[String]) = {

      // Parse JSON into tweet records.  Filter out nulls (unparsable tweets).
      val values_extracted = lines.map(parse_json).filter(_ != null)

      /* Filter duplicates, invalid tweets, tweets not matching any
         tweet-level boolean filters. (User-level boolean filters get
         applied later.) */
      filter_tweets(values_extracted.filter(is_valid_tweet))
    }
  }

  class GroupTweets(opts: ParseTweetsParams)
      extends ParseTweetsAction {

    val operation_category = "Group"

    lazy val filter_groups_ast =
      create_parser(opts.filter_groups, foldcase = true)
    lazy val cfilter_groups_ast =
      create_parser(opts.cfilter_groups, foldcase = false)

    /**
     * Apply any boolean filters given in `--filter-groups` or
     * `--cfilter-groups`.
     */
    def filter_tweet_by_group_filters(tweet: Tweet) = {
      (filter_groups_ast == null || (filter_groups_ast matches tweet)) &&
       (cfilter_groups_ast == null || (cfilter_groups_ast matches tweet))
    }

    def tweet_to_record(tweet: Tweet) = {
      val key = opts.grouping match {
        case "user" => tweet.user
        case "time" =>
          ((tweet.min_timestamp / opts.timeslice) * opts.timeslice).toString
        case "none" => tweet.id.toString
      }
      Record(key, filter_tweet_by_group_filters(tweet), tweet)
    }

    /**
     * Merge the data associated with two tweets or tweet combinations
     * into a single tweet combination.  Concatenate text.  Find maximum
     * numbers of followers and followees.  Add number of tweets in each.
     * For latitude and longitude, take the earliest provided values
     * ("earliest" by timestamp and "provided" meaning not missing).
     */
    def merge_records(tw1: Record, tw2: Record): Record = {
      assert(tw1.key == tw2.key)
      val t1 = tw1.tweet
      val t2 = tw2.tweet
      val id = if (t1.id != t2.id) -1L else t1.id
      val lang = if (t1.lang != t2.lang) "[multiple]" else t1.lang
      val (followers, following) =
        (math.max(t1.followers, t2.followers),
         math.max(t1.following, t2.following))
      val text = t1.text ++ t2.text
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
        Tweet("", text, t1.user, id, min_timestamp, max_timestamp,
          geo_timestamp, lat, long, followers, following, lang, numtweets,
          user_mentions, retweets, hashtags, urls)
      Record(tw1.key, tw1.matches || tw2.matches, tweet)
    }

    /**
     * Return true if tweet (combination) has a fully-specified latitude
     * and longitude.
     */
    def has_latlong(tw: Tweet) = {
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
    def is_nonspammer(tw: Tweet): Boolean = {
      val retval =
        (tw.following >= MIN_NUMBER_FOLLOWING &&
           tw.following <= MAX_NUMBER_FOLLOWING) &&
        (tw.followers >= MIN_NUMBER_FOLLOWERS) &&
        (tw.numtweets >= opts.min_tweets && tw.numtweets <= opts.max_tweets)
      if (opts.debug && retval == false)
        logger.info("Rejecting is_nonspammer %s" format tw)
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
    def northamerica_only(tw: Tweet): Boolean = {
      val retval = (tw.lat >= MIN_LAT && tw.lat <= MAX_LAT) &&
                   (tw.long >= MIN_LNG && tw.long <= MAX_LNG)
      if (opts.debug && retval == false)
        logger.info("Rejecting northamerica_only %s" format tw)
      retval
    }

    def is_good_geo_tweet(tw: Tweet): Boolean = {
      if (opts.debug)
        logger.info("Considering %s" format tw)
      has_latlong(tw) &&
      is_nonspammer(tw) &&
      northamerica_only(tw)
    }

    def apply(tweets: DList[Tweet]) = {
      // Group by grouping key (username, timestamp, etc.).  username, then combine the records for a user into a
      // tweet combination, with text concatenated and the location taken
      // from the earliest tweet with a specific coordinate; then filter
      // according to group-level user filters and convert back to tweets,
      // since we have no more need of the extra record-level info.
      val concatted = tweets.
        map(tweet_to_record).   // convert to Record (which contains grouping
                                // key and group-level filter result)
        groupBy(_.key).         // group on grouping key
        combine(merge_records). // merge value tweet records
        filter(_._2.matches).   // apply group-level user filters
        map(_._2.tweet)         // convert back to Tweet

      // If grouping by user, filter the tweet combinations, removing users
      // without a specific coordinate; users that appear to be "spammers" or
      // other users with non-standard behavior; and users located outside of
      // North America.  FIXME: We still want to filter spammers; but this
      // is trickier when not grouping by user.  How to do it?
      if (opts.geographic_only) concatted.filter(is_good_geo_tweet)
      else concatted
    }
  }

  class TokenizeCountAndFormat(opts: ParseTweetsParams)
      extends ParseTweetsAction {

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
    def tokenize_count_and_format(tweet: Tweet): String = {
      val encoded_tweet = tweet.to_row
      val formatted_text = emit_ngrams(tweet.text)
      encoded_tweet + "\t" + formatted_text
    }
  }

  /**
   * @param ty Type of value
   * @param key2 Second-level key to group on; Typically, either we want the
   *   individual value to appear in the overall stats (at 2nd level) or not;
   *   when not, put the individual value in `value`, and put the value of
   *   `ty` in `key2`; otherwise, put the individual value in `key2`, and
   *   usually put a constant value (e.g. "") in `value` so all relevant
   *   tweets get grouped together.
   * @param value See above.
   */
  case class FeatureValueStats(
    ty: String,
    key2: String,
    value: String,
    num_tweets: Int,
    min_timestamp: Timestamp,
    max_timestamp: Timestamp
  ) {
    def to_row(opts: ParseTweetsParams) = {
      import Encoder._
      Seq(
        string(ty),
        string(key2),
        string(value),
        int(num_tweets),
        timestamp(min_timestamp),
        timestamp(max_timestamp)
      ) mkString "\t"
    }
  }

  implicit val featureValueStatsWire =
    mkCaseWireFormat(FeatureValueStats.apply _, FeatureValueStats.unapply _)

  object FeatureValueStats {
    def row_fields =
      Seq(
        "type",
        "key2",
        "value",
        "num-tweets",
        "min-timestamp",
        "max-timestamp")

    def from_row(row: String, opts: ParseTweetsParams) = {
      import Decoder._
      val Array(ty, key2, value, num_tweets, min_timestamp, max_timestamp) =
        row.split("\t", -1)
      FeatureValueStats(
        string(ty),
        string(key2),
        string(value),
        int(num_tweets),
        timestamp(min_timestamp),
        timestamp(max_timestamp)
      )
    }

    def from_tweet(tweet: Tweet, ty: String, key2: String, value: String) = {
      FeatureValueStats(ty, key2, value, 1, tweet.min_timestamp,
        tweet.max_timestamp)
    }

    def merge_stats(x1: FeatureValueStats, x2: FeatureValueStats) = {
      assert(x1.ty == x2.ty)
      assert(x1.key2 == x2.key2)
      assert(x1.value == x2.value)
      FeatureValueStats(x1.ty, x1.key2, x1.value,
        x1.num_tweets + x2.num_tweets,
        math.min(x1.min_timestamp, x2.min_timestamp),
        math.max(x1.max_timestamp, x2.max_timestamp))
    }
  }

  /**
   * Statistics on any tweet "feature" (e.g. user, language) that can be
   * identified by a value of some type (e.g. string, number) and has an
   * associated map of occurrences of values of the feature.
   */
  case class FeatureStats(
    ty: String,
    key2: String,
    lowest_value_by_sort: String,
    highest_value_by_sort: String,
    most_common_value: String,
    most_common_count: Int,
    least_common_value: String,
    least_common_count: Int,
    num_value_types: Int,
    num_value_occurrences: Int
  ) extends Ordered[FeatureStats] {
    def compare(that: FeatureStats) = {
      (ty compare that.ty) match {
        case 0 => key2 compare that.key2
        case x => x
      }
    }

    def to_row(opts: ParseTweetsParams) = {
      import Encoder._
      Seq(
        string(ty),
        string(key2),
        string(lowest_value_by_sort),
        string(highest_value_by_sort),
        string(most_common_value),
        int(most_common_count),
        string(least_common_value),
        int(least_common_count),
        int(num_value_types),
        int(num_value_occurrences),
        double(num_value_occurrences.toDouble/num_value_types)
      ) mkString "\t"
    }
  }

  implicit val featureStatsWire =
    mkCaseWireFormat(FeatureStats.apply _, FeatureStats.unapply _)

  object FeatureStats {
    def row_fields =
      Seq(
        "type",
        "key2",
        "lowest-value-by-sort",
        "highest-value-by-sort",
        "most-common-value",
        "most-common-count",
        "least-common-value",
        "least-common-count",
        "num-value-types",
        "num-value-occurrences",
        "avg-value-occurrences"
      )

    def from_row(row: String) = {
      import Decoder._
      val Array(ty, key2, lowest_value_by_sort, highest_value_by_sort,
        most_common_value, most_common_count,
        least_common_value, least_common_count,
        num_value_types, num_value_occurrences, avo) =
        row.split("\t", -1)
      FeatureStats(
        string(ty),
        string(key2),
        string(lowest_value_by_sort),
        string(highest_value_by_sort),
        string(most_common_value),
        int(most_common_count),
        string(least_common_value),
        int(least_common_count),
        int(num_value_types),
        int(num_value_occurrences)
      )
    }

    def from_value_stats(vs: FeatureValueStats) =
      FeatureStats(vs.ty, vs. key2, vs.value, vs.value, vs.value, vs.num_tweets,
      vs.value, vs.num_tweets, 1, vs.num_tweets)

    def merge_stats(x1: FeatureStats, x2: FeatureStats) = {
      assert(x1.ty == x2.ty)
      assert(x1.key2 == x2.key2)
      val (most_common_value, most_common_count) =
        if (x1.most_common_count > x2.most_common_count)
          (x1.most_common_value, x1.most_common_count)
        else
          (x2.most_common_value, x2.most_common_count)
      val (least_common_value, least_common_count) =
        if (x1.least_common_count < x2.least_common_count)
          (x1.least_common_value, x1.least_common_count)
        else
          (x2.least_common_value, x2.least_common_count)
      FeatureStats(x1.ty, x1.key2,
        if (x1.lowest_value_by_sort < x2.lowest_value_by_sort)
          x1.lowest_value_by_sort
        else x2.lowest_value_by_sort,
        if (x1.highest_value_by_sort > x2.highest_value_by_sort)
          x1.highest_value_by_sort
        else x2.highest_value_by_sort,
        most_common_value, most_common_count,
        least_common_value, least_common_count,
        x1.num_value_types + x2.num_value_types,
        x1.num_value_occurrences + x2.num_value_occurrences)
    }
  }

  class GetStats(opts: ParseTweetsParams)
      extends ParseTweetsAction {

    import java.util.Calendar

    val operation_category = "GetStats"

    val date_fmts = Seq(
      ("year", "yyyy"),                   // Ex: "2012"
      ("year/month", "yyyy-MM (MMM)"),    // Ex. "2012-07 (Jul)"
      ("year/month/day",
        "yyyy-MM-dd (MMM d)"),            // Ex. "2012-07-05 (Jul 5)"
      ("month", "'month' MM (MMM)"),      // Ex. "month 07 (Jul)"
      ("month/week",
        "'month' MM (MMM), 'week' W"),    // Ex. "month 07 (Jul), week 2"
      ("month/day", "MM-dd (MMM d)"),     // Ex. "07-05 (Jul 5)"
      ("weekday", "'weekday' 'QQ' (EEE)"),// Ex. "weekday 2 (Mon)"
      ("hour", "HHaa"),                   // Ex. "09am"
      ("weekday/hour",
         "'weekday' 'QQ' (EEE), HHaa"),   // Ex. "weekday 2 (Mon), 23pm"
      ("hour/weekday",
        "HHaa, 'weekday' 'QQ' (EEE)"),    // Ex. "23pm, weekday 2 (Mon)"
      ("weekday/month", "'weekday' 'QQ' (EEE), 'month' MM (MMM)"),
                                    // Ex. "weekday 5 (Thu), month 07 (Jul)"
      ("month/weekday", "'month' MM (MMM), 'weekday' 'QQ' (EEE)")
                                   // Ex. "month 07 (Jul), weekday 5 (Thu)"
    )
    lazy val calinst = Calendar.getInstance
    lazy val sdfs =
      for ((engl, fmt) <- date_fmts) yield (engl, new SimpleDateFormat(fmt))

    /**
     * Format a timestamp according to a date format.  This would be easy if
     * not for the fact that SimpleDateFormat provides no way of inserting
     * the numeric equivalent of a day of the week, which we want in order
     * to make sorting turn out correctly.  So we have to retrieve it using
     * the `Calendar` class and shoehorn it in wherever the non-code QQ
     * was inserted.
     *
     * NOTE, a quick guide to Java date-related classes:
     *
     * java.util.Date: A simple wrapper around a timestamp in "Epoch time",
     *   i.e. milliseconds after the Unix Epoch of Jan 1, 1970, 00:00:00 GMT.
     *   Formerly also used for converting timestamps into human-style
     *   dates, but all that stuff is long deprecated because of lack of
     *   internationalization support.
     * java.util.Calendar: A class that supports conversion between timestamps
     *   and human-style dates, e.g. to figure out the year, month and day of
     *   a given timestamp.  Supports time zones, daylight savings oddities,
     *   etc.  Subclasses are supposed to represent different calendar systems
     *   but in reality there's only one, named GregorianCalendar but which
     *   actually supports both Gregorian (modern) and Julian (old-style,
     *   with no leap-year special-casing of years divisible by 100) calendars,
     *   with a configurable cross-over point.  Support for other calendars
     *   (Islamic, Jewish, etc.) is provided by third-party libraries (e.g.
     *   JodaTime), which typically discard the java.util.Calendar framework
     *   and create their own.
     * java.util.DateFormat: A class that supports conversion between
     *   timestamps and human-style dates nicely formatted into a string, e.g.
     *   generating strings like "Jul 23, 2012 08:05pm".  Again, theoretically
     *   there are particular subclasses to support different formatting
     *   mechanisms but in reality there's only one, SimpleDateFormat.
     *   Readable dates are formatted using a template, and there's a good
     *   deal of localization-specific stuff under the hood that theoretically
     *   the programmer doesn't need to worry about.
     */
    def format_date(time: Timestamp, fmt: SimpleDateFormat) = {
      val output = fmt.format(new Date(time))
      calinst.setTimeInMillis(time)
      val weekday = calinst.get(Calendar.DAY_OF_WEEK)
      output.replace("QQ", weekday.toString)
    }
      
    def stats_for_tweet(tweet: Tweet) = {
      Seq(FeatureValueStats.from_tweet(tweet, "user", "user", tweet.user),
          // Get a summary for all languages plus a summary for each lang
          FeatureValueStats.from_tweet(tweet, "lang", tweet.lang, ""),
          FeatureValueStats.from_tweet(tweet, "lang", "lang", tweet.lang)) ++
        sdfs.map { case (engl, fmt) =>
          FeatureValueStats.from_tweet(
            tweet, engl, format_date(tweet.min_timestamp, fmt), "") }
    }

    /**
     * Compute statistics on a DList of tweets.
     */
    def get_by_value(tweets: DList[Tweet]) = {
      /* Operations:

         1. For each tweet, and for each feature we're interested in getting
            stats on (e.g. users, languages, etc.), generate a tuple
            (keytype, key, value) that has the type of feature as `keytype`
            (e.g. "user"), the value of the feature in `key` (e.g. the user
            name), and some sort of stats object (e.g. `FeatureStats`),
            giving statistics on that user (etc.) derived from the individual
            tweet.

         2. Take the resulting DList and group by grouping key.  Combine the
            resulting `FeatureStats` together by adding their values or
            taking max/min or whatever. (If there are multiple feature types,
            we might have multiple classes involved, so we need to condition
            on the feature type.)

         3. The resulting DList has one entry per feature value, giving
            stats on all tweets corresponding to that feature value.  We
            want to aggregate again of feature type, to get statistics on
            the whole type (e.g. how many different feature values, how
            often they occur).
       */
      tweets.flatMap(x => stats_for_tweet(x)).
      groupBy({ stats => (stats.ty, stats.key2, stats.value)}).
      combine(FeatureValueStats.merge_stats).
      map(_._2)
    }

    def get_by_type(values: DList[FeatureValueStats]) = {
      values.map(FeatureStats.from_value_stats(_)).
      groupBy({ stats => (stats.ty, stats.key2) }).
      combine(FeatureStats.merge_stats).
      map(_._2)
    }
  }

  class ParseTweetsDriver(opts: ParseTweetsParams)
      extends ParseTweetsAction {

    val operation_category = "Driver"

    def corpus_suffix = {
      val dist_type = if (opts.max_ngram == 1) "unigram" else "ngram"
      "%s-%s-counts-tweets" format (opts.split, dist_type)
    }

    /**
     * Output a schema file of the appropriate name.
     */
    def output_schema(filehand: FileHandler) {
      val filename = Schema.construct_schema_file(filehand,
        opts.output, opts.corpus_name, corpus_suffix)
      logger.info("Outputting a schema to %s ..." format filename)
      // We add the counts data to what to_row() normally outputs so we
      // have to add the same field here
      val fields = Tweet.row_fields ++ Seq("counts")
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

  def create_params(ap: ArgParser) = new ParseTweetsParams(ap)
  val progname = "ParseTweets"

  def run() {
    val opts = init_scoobi_app()
    val filehand = new HadoopFileHandler(configuration)
    if (opts.corpus_name == null) {
      val (_, last_component) = filehand.split_filename(opts.input)
      opts.corpus_name = last_component.replace("*", "_")
    }
    errprint("ParseTweets: " + (opts.grouping match {
      case "time" =>
        "grouping by time, with slices of %g seconds".
          format(opts.timeslice_float)
      case "user" =>
        "grouping by user"
      case "none" =>
        "not grouping"
    }))
    errprint("ParseTweets: " + (opts.output_format match {
      case "corpus" =>
        "outputting tweets as a TextGrounder corpus"
      case "raw" =>
        "outputting tweets as raw JSON"
      case "stats" =>
        "outputting statistics on tweets"
    }))
    val ptp = new ParseTweetsDriver(opts)

    // Firstly we load up all the (new-line-separated) JSON lines.
    val lines: DList[String] = TextInput.fromTextFile(opts.input)

    errprint("ParseTweets: Generate tweets ...")
    val tweets1 = new ParseAndUniquifyTweets(opts)(lines)

    /* Maybe group tweets */
    val tweets =
      if (opts.grouping == "none") tweets1
      else new GroupTweets(opts)(tweets1)

    def dlist_output_lines(lines: DList[String], corpus_suffix: String,
        fields: Seq[String]) = {
      val outdir = opts.output + "-" + corpus_suffix
      persist(TextOutput.toTextFile(lines, outdir))
      val out_schema = new Schema(fields, Map("corpus" -> opts.corpus_name))
      val out_schema_fn = Schema.construct_schema_file(filehand,
          outdir, opts.corpus_name, corpus_suffix)
      rename_output_files(outdir, opts.corpus_name, corpus_suffix)
      out_schema.output_schema_file(filehand, out_schema_fn)
      outdir
    }

    def local_output_lines(lines: Iterable[String], corpus_suffix: String,
        fields: Seq[String]) = {
      val outdir = opts.output + "-" + corpus_suffix
      filehand.make_directories(outdir)
      val out_schema = new Schema(fields, Map("corpus" -> opts.corpus_name))
      val out_schema_file = Schema.construct_schema_file(filehand,
          outdir, opts.corpus_name, corpus_suffix)
      out_schema.output_schema_file(filehand, out_schema_file)
      val outfile = CorpusFileProcessor.construct_output_file(filehand, outdir,
        opts.corpus_name, corpus_suffix, ".txt")
      val outstr = filehand.openw(outfile)
      lines.map(outstr.println(_))
      outstr.close()
      outdir
    }

    def rename_outfiles() {
      rename_output_files(opts.output, opts.corpus_name, ptp.corpus_suffix)
    }

    opts.output_format match {
      case "raw" => {
        /* If we're outputting raw, output the JSON of the tweet. */
        persist(TextOutput.toTextFile(tweets.map(_.json), opts.output))
        rename_outfiles()
      }
      case "corpus" => {
        val tfct = new TokenizeCountAndFormat(opts)
        // Tokenize the combined text into words, possibly generate ngrams
        // from them, count them up and output results formatted into a record.
        val nicely_formatted = tweets.map(tfct.tokenize_count_and_format)
        persist(TextOutput.toTextFile(nicely_formatted, opts.output))
        rename_outfiles()
        // create a schema
        ptp.output_schema(filehand)
      }
      case "stats" => {
        val get_stats = new GetStats(opts)
        val by_value = get_stats.get_by_value(tweets)
        val dlist_by_type = get_stats.get_by_type(by_value)
        val by_type = persist(dlist_by_type.materialize).toSeq.sorted
        val stats_suffix = "stats"
        local_output_lines(by_type.map(_.to_row(opts)),
          stats_suffix, FeatureStats.row_fields)
        val userstat = by_type.filter(x =>
          x.ty == "user" && x.key2 == "user").toSeq(0)
        errprint("\nCombined summary:")
        errprint("%s tweets by %s users = %.2f tweets/user",
          with_commas(userstat.num_value_occurrences),
          with_commas(userstat.num_value_types),
          userstat.num_value_occurrences.toDouble / userstat.num_value_types)
        val monthstat = by_type.filter(_.ty == "year/month")
        errprint("\nSummary by month:")
        for (mo <- monthstat)
          errprint("%-20s: %12s tweets", mo.key2,
            with_commas(mo.num_value_occurrences))
        val daystat = by_type.filter(_.ty == "year/month/day")
        errprint("\nSummary by day:")
        for (d <- daystat)
          errprint("%-20s: %12s tweets", d.key2,
            with_commas(d.num_value_occurrences))
        errprint("\n")
      }
    }

    errprint("ParseTweets: done.")

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

