///////////////////////////////////////////////////////////////////////////////
//  TextUtil.scala
//
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

package opennlp.textgrounder.util

import scala.util.control.Breaks._
import scala.util.matching.Regex
import math._

import printutil.warning

package object textutil {

  ////////////////////////////////////////////////////////////////////////////
  //                    String functions involving numbers                  //
  ////////////////////////////////////////////////////////////////////////////

  /**
   Convert a string to floating point, but don't crash on errors;
  instead, output a warning.
   */
  def safe_float(x: String) = {
    try {
      x.toDouble
    } catch {
      case _ => {
        val y = x.trim()
        if (y != "") warning("Expected number, saw %s", y)
        0.
      }
    }
  }

  // Originally based on code from:
  // http://stackoverflow.com/questions/1823058/how-to-print-number-with-commas-as-thousands-separators-in-python-2-x
  def with_commas(x: Long): String = {
    var mx = x
    if (mx < 0)
      "-" + with_commas(-mx)
    else {
      var result = ""
      while (mx >= 1000) {
        val r = mx % 1000
        mx /= 1000
        result = ",%03d%s" format (r, result)
      }
      "%d%s" format (mx, result)
    }
  }
  
  // My own version
  def with_commas(x: Double): String = {
    val intpart = floor(x).toInt
    val fracpart = x - intpart
    with_commas(intpart) + ("%.2f" format fracpart).drop(1)
  }

  /**
   * Try to format a floating-point number using %f style (i.e. avoding
   * scientific notation) and with a fixed number of significant digits
   * after the decimal point.
   *
   * @param sigdigits Number of significant digits after decimal point
   *   to display.
   * @param include_plus If true, include a + sign before positive numbers.
   */
  def format_float(x: Double, sigdigits: Int = 2,
      include_plus: Boolean = false) = {
    var precision = sigdigits
    if (x != 0) {
      var xx = abs(x)
      while (xx < 0.1) {
        xx *= 10
        precision += 1
      }
    }
    val formatstr =
      "%%%s.%sf" format (if (include_plus) "+" else "", precision)
    formatstr format x
  }

  ////////////////////////////////////////////////////////////////////////////
  //                           Other string functions                       //
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Split a string, similar to `str.split(delim_re, -1)`, but also
   * return the delimiters.  Return an Iterable of tuples `(text, delim)`
   * where `delim` is the delimiter following each section of text.  The
   * last delimiter will be an empty string.
   */
  def split_with_delim(str: String, delim_re: Regex):
      Iterable[(String, String)] = {
    // Find all occurrences of regexp, extract start and end positions,
    // flatten, and add suitable positions for the start and end of the string.
    // Adding the end-of-string position twice ensures that we get the empty
    // delimiter at the end.
    val delim_intervals =
      Iterator(0) ++
      delim_re.findAllIn(str).matchData.flatMap(m => Iterator(m.start, m.end)) ++
      Iterator(str.length, str.length)
    // Group into (start, end) pairs for both text and delimiter, extract
    // the strings, group into text-delimiter tuples and convert to Iterable.
    delim_intervals sliding 2 map {
      case Seq(start, end) => str.slice(start, end)
    } grouped 2 map {
      case Seq(text, delim) => (text, delim)
    } toIterable
  }

  def split_text_into_words(text: String, ignore_punc: Boolean=false,
    include_nl: Boolean=false) = {
    // This regexp splits on whitespace, but also handles the following cases:
    // 1. Any of , ; . etc. at the end of a word
    // 2. Parens or quotes in words like (foo) or "bar"
    // These punctuation characters are returned as separate words, unless
    // 'ignore_punc' is given.  Also, if 'include_nl' is given, newlines are
    // returned as their own words; otherwise, they are treated like all other
    // whitespace (i.e. ignored).
    (for ((word, punc) <-
          split_with_delim(text, """([,;."):]*(?:\s+|$)[("]*)""".r)) yield
       Seq(word) ++ (
         for (p <- punc; if !(" \t\r\f\013" contains p)) yield (
           if (p == '\n') (if (include_nl) p.toString else "")
           else (if (!ignore_punc) p.toString else "")
         )
       )
    ) reduce (_ ++ _) filter (_ != "")
  }
 
  
  /**
   Pluralize an English word, using a basic but effective algorithm.
   */
  def pluralize(word: String) = {
    val upper = word.last >= 'A' && word.last <= 'Z'
    val lowerword = word.toLowerCase()
    val ies_re = """.*[b-df-hj-np-tv-z]y$""".r
    val es_re = """.*([cs]h|[sx])$""".r
    lowerword match {
      case ies_re() =>
        if (upper) word.dropRight(1) + "IES"
        else word.dropRight(1) + "ies"
      case es_re() =>
        if (upper) word + "ES"
        else word + "es"
      case _ =>
        if (upper) word + "S"
        else word + "s"
    }
  }

  /**
   Capitalize the first letter of string, leaving the remainder alone.
   */
  def capfirst(st: String) = {
    if (st == "") st else st(0).toString.capitalize + st.drop(1)
  }
  
  /*
    A simple object to make regexps a bit less awkward.  Works like this:

    ("foo (.*)", "foo bar") match {
      case Re(x) => println("matched 1 %s" format x)
      case _ => println("no match 1")
    }

    This will print out "matched 1 bar".
   */

  object Re {
    def unapplySeq(x: Tuple2[String, String]) = {
      val (re, str) = x
      re.r.unapplySeq(str)
    }
  }
}

