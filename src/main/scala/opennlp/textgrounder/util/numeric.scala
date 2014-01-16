///////////////////////////////////////////////////////////////////////////////
//  numeric.scala
//
//  Copyright (C) 2011-2014 Ben Wing, The University of Texas at Austin
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

import scala.math._

import math.is_negative
// import error.warning

////////////////////////////////////////////////////////////////////////////
//                    String functions involving numbers                  //
////////////////////////////////////////////////////////////////////////////

package object numeric {
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
}

