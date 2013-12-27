///////////////////////////////////////////////////////////////////////////////
//  TextUtil.scala
//
//  Copyright (C) 2011-2013 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package util

import scala.util.control.Breaks._
import scala.util.matching.Regex
import scala.math._

import math.is_negative
import print.warning

package object text {

  ////////////////////////////////////////////////////////////////////////////
  //                    String functions involving numbers                  //
  ////////////////////////////////////////////////////////////////////////////

//  /**
//   Convert a string to floating point, but don't crash on errors;
//  instead, output a warning.
//   */
//  def safe_float(x: String) = {
//    try {
//      x.toDouble
//    } catch {
//      case _: Exception => {
//        val y = x.trim()
//        if (y != "") warning("Expected number, saw %s", y)
//        0.0
//      }
//    }
//  }

  // Originally based on code from:
  // http://stackoverflow.com/questions/1823058/how-to-print-number-with-commas-as-thousands-separators-in-python-2-x
  protected def imp_format_long_commas(x: Long): String = {
    var mx = x
    if (mx < 0)
      "-" + imp_format_long_commas(-mx)
    else {
      var result = ""
      while (mx >= 1000) {
        val r = mx % 1000
        mx /= 1000
        result = ",%03d%s" format (r, result)
      }
      "%s%s" format (mx, result)
    }
  }

  /**
   * Format a long, optionally adding commas to separate thousands.
   *
   * @param with_commas If true, add commas to separate thousands.
   * @param include_plus If true, include a + sign before positive numbers.
   */
  def format_long(x: Long, include_plus: Boolean = false,
      with_commas: Boolean = false) = {
    val sign = if (include_plus && x >= 0) "+" else ""
    sign + (
      if (!with_commas) x.toString
      else imp_format_long_commas(x)
    )
  }

  /**
   * Format a long, adding commas to separate thousands.
   *
   * @param include_plus If true, include a + sign before positive numbers.
   */
  def format_long_commas(x: Long, include_plus: Boolean = false) =
    format_long(x, include_plus = include_plus, with_commas = true)

  /**
   * Format a long integer in a "pretty" fashion. This adds commas
   * to separate thousands in the integral part.
   *
   * @param sigdigits Number of significant digits after decimal point
   *   to display.
   * @param include_plus If true, include a + sign before positive numbers.
   */
  def pretty_long(x: Long, include_plus: Boolean = false) =
    format_long_commas(x, include_plus = include_plus)

  /**
   * Format a floating-point number using %f style (i.e. avoding
   * scientific notation) and with a fixed number of significant digits
   * after the decimal point. Normally this is the same as the actual
   * number of digits displayed after the decimal point, but more digits
   * will be used if the number is excessively small (e.g. possible outputs
   * might be 0.33, 0.033, 0.0033, etc. for 1.0/3, 1.0/30, 1.0/300, etc.).
   *
   * @param sigdigits Number of significant digits after decimal point
   *   to display.
   * @param include_plus If true, include a + sign before positive numbers.
   * @param drop_zeros If true, drop trailing zeros after decimal point.
   * @param with_commas If true, add commas to separate thousands
   * in the integral part.
   */
  def format_double(x: Double, sigdigits: Int = 2,
      drop_zeros: Boolean = false,
      with_commas: Boolean = false,
      include_plus: Boolean = false): String = {
    if (is_negative(x)) {
      // Don't use x.abs because it has a bug handling -0.0
      "-" + format_double(abs(x), sigdigits = sigdigits,
        drop_zeros = drop_zeros, with_commas = with_commas,
        include_plus = false)
    } else if (with_commas) {
      val sign = if (include_plus) "+" else ""
      val longpart = x.toLong
      // Use 1+ so that we don't get special treatment of values near 0
      // unless we're actually near 0
      val fracpart = (if (longpart != 0) 1 else 0) + abs(x - longpart)
      sign + imp_format_long_commas(longpart) +
        format_double(fracpart, sigdigits = sigdigits,
          drop_zeros = drop_zeros, with_commas = false,
          include_plus = false).
        drop(1)
    } else {
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
      val retval = formatstr format x
      if (drop_zeros)
        // Drop zeros after decimal point, then drop decimal point if it's last.
        retval.replaceAll("""\.([0-9]*?)0+$""", """.$1""").
          replaceAll("""\.$""", "")
      else
        retval
    }
  }

  /**
   * Format a floating-point number, dropping final zeros so it takes the
   * minimum amount of space.
   *
   * @param sigdigits Number of significant digits after decimal point
   *   to display.
   * @param include_plus If true, include a + sign before positive numbers.
   */
  def min_format_double(x: Double, sigdigits: Int = 2,
      with_commas: Boolean = false,
      include_plus: Boolean = false) =
    format_double(x, sigdigits = sigdigits, with_commas = with_commas,
      include_plus = include_plus, drop_zeros = true)

  /**
   * Format a floating-point number, adding commas to separate thousands
   * in the integral part.
   *
   * @param sigdigits Number of significant digits after decimal point
   *   to display.
   * @param include_plus If true, include a + sign before positive numbers.
   */
  def format_double_commas(x: Double, sigdigits: Int = 2,
      drop_zeros: Boolean = false,
      include_plus: Boolean = false) =
    format_double(x, sigdigits = sigdigits, include_plus = include_plus,
      drop_zeros = drop_zeros, with_commas = true)

  /**
   * Format a floating-point number, dropping final zeros so it takes the
   * minimum amount of space and adding commas to separate thousands in the
   * integral part.
   *
   * @param sigdigits Number of significant digits after decimal point
   *   to display.
   * @param include_plus If true, include a + sign before positive numbers.
   */
  def min_format_double_commas(x: Double, sigdigits: Int = 2,
      include_plus: Boolean = false) =
    format_double(x, sigdigits = sigdigits, include_plus = include_plus,
      drop_zeros = true, with_commas = true)

  /**
   * Format a floating-point number in a "pretty" fashion. This drops
   * final zeros so it takes the minimum amount of space and adds commas
   * to separate thousands in the integral part.
   *
   * @param sigdigits Number of significant digits after decimal point
   *   to display.
   * @param include_plus If true, include a + sign before positive numbers.
   */
  def pretty_double(x: Double, sigdigits: Int = 2,
      include_plus: Boolean = false) =
    min_format_double_commas(x, sigdigits = sigdigits,
      include_plus = include_plus)

  ////////////////////////////////////////////////////////////////////////////
  //                           Table-output functions                       //
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Return the printf-style format for formatting a table consisting of a
   * series of lines, each of which contains a series of columns. First
   * computes the maximum size of each column and then formats appropriately
   * so everything fits.
   *
   * @param lines Lines of column values to format.
   * @param maxcolsize Maximum size of a column.
   */
  def table_column_format(lines: Iterable[Iterable[String]],
      maxcolsize: Int = 40) = {
    val maxsize = lines.transpose.map { line =>
      line.map { _.length}.max min maxcolsize
    }
    maxsize.map { size => s"%-${size}s" } mkString " "
  }

  /**
   * Format a table consisting of a series of lines, each of which contains
   * a series of columns. First computes the maximum size of each column
   * and then formats appropriately so everything fits.
   *
   * @param lines Lines of column values to format.
   * @param maxcolsize Maximum size of a column.
   */
  def format_table(lines: Iterable[Iterable[String]], maxcolsize: Int = 40) = {
    val fmt = table_column_format(lines, maxcolsize)
    lines.map { line => fmt.format(line.toSeq: _*) } mkString "\n"
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

  def split_text_into_words(text: String, ignore_punc: Boolean = false,
      include_nl: Boolean = false) = {
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
}

package text {
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

