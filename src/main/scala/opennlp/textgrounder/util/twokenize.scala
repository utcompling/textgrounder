package opennlp.textgrounder.util

/* TextGrounder note:

   This has been taken directly from Jason Baldridge's twokenize.scala
   version as of 2011-06-13, from here:

   https://bitbucket.org/jasonbaldridge/twokenize

   The only change so far has been adding this comment and the above
   package statement.

   FIXME: Make Twokenize be a package retrievable by Maven.

   - Ben Wing (ben@benwing.com) November 2011
*/

/*
 TweetMotif is licensed under the Apache License 2.0: 
 http://www.apache.org/licenses/LICENSE-2.0.html
 Copyright Brendan O'Connor, Michel Krieger, and David Ahn, 2009-2010.
*/

/*

 Scala port of Brendar O' Connor's twokenize.py

 This is not a direct port, as some changes were made in the aim of
 simplicity.  In the Python version, the @tokenize@ method returned a
 Tokenization object which wrapped a Python List with some extra
 methods.

 The @tokenize@ method given here receives a String and returns the
 tokenized Array[String] of the input text Twokenize.tokenize("foobar
 baz.") => ['foobar', 'baz', '.']

 The main method reads from stdin like it's python counterpart and
 calls the above method on each line

  > scalac twokenize.scala
  > echo "@foo #bar \$1.00 isn't 8:42 p.m. Mr. baz." | scala Twokenize
  @foo $1.0 #bar baz .

 - David Snyder (dsnyder@cs.utexas.edu)
   April 2011

 Modifications to more functional style, fix a few bugs, and making
 output more like twokenize.py. Added abbrevations. Tweaked some
 regex's to produce better tokens.

 - Jason Baldridge (jasonbaldridge@gmail.com)
   June 2011
*/

import scala.util.matching.Regex

object Twokenize {

  val Contractions = """(?i)(\w+)(n't|'ve|'ll|'d|'re|'s|'m)$""".r
  val Whitespace = """\s+""".r

  val punctChars = """['“\".?!,:;]"""
  val punctSeq   = punctChars+"""+"""
  val entity     = """&(amp|lt|gt|quot);"""

  //  URLs

  // David: I give the Larry David eye to this whole URL regex
  // (http://www.youtube.com/watch?v=2SmoBvg-etU) There are
  // potentially better options, see:
  //   http://daringfireball.net/2010/07/improved_regex_for_matching_urls
  //   http://mathiasbynens.be/demo/url-regex

  val urlStart1  = """(https?://|www\.)"""
  val commonTLDs = """(com|co\.uk|org|net|info|ca|ly)"""
  val urlStart2  = """[A-Za-z0-9\.-]+?\.""" + commonTLDs + """(?=[/ \W])"""
  val urlBody    = """[^ \t\r\n<>]*?"""
  val urlExtraCrapBeforeEnd = "("+punctChars+"|"+entity+")+?"
  val urlEnd     = """(\.\.+|[<>]|\s|$)"""
  val url        = """\b("""+urlStart1+"|"+urlStart2+")"+urlBody+"(?=("+urlExtraCrapBeforeEnd+")?"+urlEnd+")"

  // Numeric
  val timeLike   = """\d+:\d+"""
  val numNum     = """\d+\.\d+"""
  val numberWithCommas = """(\d+,)+?\d{3}""" + """(?=([^,]|$))"""

  // Note the magic 'smart quotes' (http://en.wikipedia.org/wiki/Smart_quotes)
  val edgePunctChars    = """'\"“”‘’<>«»{}\(\)\[\]"""  
  val edgePunct    = "[" + edgePunctChars + "]"
  val notEdgePunct = "[a-zA-Z0-9]"
  val EdgePunctLeft  = new Regex("""(\s|^)("""+edgePunct+"+)("+notEdgePunct+")")
  val EdgePunctRight = new Regex("("+notEdgePunct+")("+edgePunct+"""+)(\s|$)""")    

  // Abbreviations
  val boundaryNotDot = """($|\s|[“\"?!,:;]|""" + entity + ")" 
  val aa1  = """([A-Za-z]\.){2,}(?=""" + boundaryNotDot + ")"
  val aa2  = """[^A-Za-z]([A-Za-z]\.){1,}[A-Za-z](?=""" + boundaryNotDot + ")"
  val standardAbbreviations = """\b([Mm]r|[Mm]rs|[Mm]s|[Dd]r|[Ss]r|[Jj]r|[Rr]ep|[Ss]en|[Ss]t)\."""
  val arbitraryAbbrev = "(" + aa1 +"|"+ aa2 + "|" + standardAbbreviations + ")"

  val separators  = "(--+|―)"
  val decorations = """[♫]+"""
  val thingsThatSplitWords = """[^\s\.,]"""
  val embeddedApostrophe = thingsThatSplitWords+"""+'""" + thingsThatSplitWords + """+"""

  //  Emoticons
  val normalEyes = "(?iu)[:=]"
  val wink = "[;]"
  val noseArea = "(|o|O|-)" // rather tight precision, \S might be reasonable...
  val happyMouths = """[D\)\]]"""
  val sadMouths = """[\(\[]"""
  val tongue = "[pP]"
  val otherMouths = """[doO/\\]""" // remove forward slash if http://'s aren't cleaned

  val emoticon = "("+normalEyes+"|"+wink+")" + noseArea + "("+tongue+"|"+otherMouths+"|"+sadMouths+"|"+happyMouths+")"
                     
  // We will be tokenizing using these regexps as delimiters
  val Protected  = new Regex(
    "(" + Array(
      emoticon,
      url,
      entity,
      timeLike,
      numNum,
      numberWithCommas,
      punctSeq,
      arbitraryAbbrev,
      separators,
      decorations,
      embeddedApostrophe
      ).mkString("|") + ")" )

  // The main work of tokenizing a tweet.
  def simpleTokenize (text: String) = {

    // Do the no-brainers first
    val splitPunctText = splitEdgePunct(text)
    val textLength = splitPunctText.length

    // Find the matches for subsequences that should be protected
    // (not further split), e.g. URLs, 1.0, U.N.K.L.E., 12:53
    val matches = Protected.findAllIn(splitPunctText).matchData.toList

    // The protected spans should not be split.
    val protectedSpans = matches map (mat => Tuple2(mat.start, mat.end))

    // Create a list of indices to create the "splittables", which can be
    // split. We are taking protected spans like 
    //     List((2,5), (8,10)) 
    // to create 
    ///    List(0, 2, 5, 8, 10, 12)
    // where, e.g., "12" here would be the textLength
    val indices =
      (0 :: (protectedSpans flatMap { case (start,end) => List(start,end) })
         ::: List(textLength))
    
    // Group the indices and map them to their respective portion of the string
    val splittableSpans =
      indices.grouped(2) map { x => splitPunctText.slice(x(0),x(1)) } toList

    //The 'splittable' strings are safe to be further tokenized by whitespace
    val splittables = splittableSpans map { str => str.trim.split(" ").toList }

    //Storing as List[List[String]] to make zip easier later on 
    val protecteds = protectedSpans map {
      case(start,end) => List(splitPunctText.slice(start,end)) }

    //  Reinterpolate the 'splittable' and 'protected' Lists, ensuring that
    //  additonal tokens from last splittable item get included
    val zippedStr = 
      (if (splittables.length == protecteds.length) 
        splittables.zip(protecteds) map { pair => pair._1 ++ pair._2 }
      else 
        ((splittables.zip(protecteds) map { pair => pair._1 ++ pair._2 }) :::
         List(splittables.last))
     ).flatten

    // Split based on special patterns (like contractions) and check all tokens are non empty
    zippedStr.map(splitToken(_)).flatten.filter(_.length > 0)
  }  

  // 'foo' => ' foo '
  def splitEdgePunct (input: String) = {
    val splitLeft  = EdgePunctLeft.replaceAllIn(input,"$1$2 $3")
    EdgePunctRight.replaceAllIn(splitLeft,"$1 $2$3")
  }

  // "foo   bar" => "foo bar"
  def squeezeWhitespace (input: String) = Whitespace.replaceAllIn(input," ").trim

  // Final pass tokenization based on special patterns
  def splitToken (token: String) = {
    token match {
      case Contractions(stem, contr) => List(stem.trim, contr.trim)
      case token => List(token.trim)
    }
  }

  // Apply method allows it to be used as Twokenize(line) in Scala.
  def apply (text: String): List[String] = simpleTokenize(squeezeWhitespace(text))

  // Named for Java coders who would wonder what the heck the 'apply' method is for.
  def tokenize (text: String): List[String] = apply(text)

  // Main method
  def main (args: Array[String]) = {
    io.Source.stdin.getLines foreach { 
      line => println(apply(line) reduceLeft(_ + " " + _)) 
    }
  }

}
