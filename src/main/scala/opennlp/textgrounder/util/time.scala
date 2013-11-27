///////////////////////////////////////////////////////////////////////////////
//  time.scala
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

import math._
import text.split_with_delim
import print.internal_error

protected class TimeImpl {

  def format_minutes_seconds(seconds: Double, hours: Boolean = true) = {
    var secs = seconds
    var mins = (secs / 60).toInt
    secs = secs % 60
    val hourstr = {
      if (!hours) ""
      else {
        val hours = (mins / 60).toInt
        mins = mins % 60
        if (hours > 0) "%s hour%s " format (hours, if (hours == 1) "" else "s")
        else ""
      }
    }
    val secstr = (if (secs.toInt == secs) "%s" else "%1.1f") format secs
    "%s%s minute%s %s second%s" format (
        hourstr,
        mins, if (mins == 1) "" else "s",
        secstr, if (secs == 1) "" else "s")
  }

  /**
   * Convert a time in milliseconds since the Epoch into a more familiar
   * format, e.g. Wed Jun 27 03:49:08 EDT 2012 in place of 1340783348365.
   */
  def format_time(millis: Long) = new java.util.Date(millis).toString

  /**
   * Convert an interval in milliseconds into a more familiar format, e.g.
   * 3m5s in place of 185000.
   */
  def format_interval(millis: Long) = {
    val sec_part_as_milli = millis % 60000
    val sec_part = sec_part_as_milli / 1000.0
    val truncated_mins = millis / 60000
    val min_part = truncated_mins % 60
    val truncated_hours = truncated_mins / 60
    val hour_part = truncated_hours % 24
    val truncated_days = truncated_hours / 24
    var res = ""
    if (res.length > 0 || truncated_days > 0)
      res += " " + truncated_days + "days"
    if (res.length > 0 || hour_part > 0)
      res += " " + hour_part + "hr"
    if (res.length > 0 || min_part > 0)
      res += " " + min_part + "min"
    if (res.length > 0 || sec_part > 0) {
      val int_sec_part = sec_part.toInt
      val secstr =
        if (int_sec_part == sec_part)
          int_sec_part
        else
          "%.2f" format sec_part
      res += " " + secstr + "sec"
    }
    if (res.length > 0 && res(0) == ' ')
      res.tail
    else
      res
  }

  /**
   * Parse a date and return a time as milliseconds since the Epoch
   * (Jan 1, 1970).  Accepts various formats, all variations of the
   * following:
   *
   * 20100802180500PST (= August 2, 2010, 18:05:00 Pacific Standard Time)
   * 2010:08:02:0605pmPST (= same)
   * 20100802:10:05pm (= same if current time zone is Eastern Daylight)
   *
   * That is, either 12-hour or 24-hour time can be given, colons can be
   * inserted anywhere for readability, and the time zone can be omitted or
   * specified.  In addition, part or all of the time of day (hours,
   * minutes, seconds) can be omitted.  Years must always be full (i.e.
   * 4 digits).
   */
  def parse_date(datestr: String): Option[Long] = {
    // Variants for the hour-minute-second portion
    val hms_variants = List("", "HH", "HHmm", "HHmmss", "hhaa", "hhmmaa",
      "hhmmssaa")
    // Fully-specified format including date
    val full_fmt = hms_variants.map("yyyyMMdd"+_)
    // All formats, including variants with time zone specified
    val all_fmt = full_fmt ++ full_fmt.map(_+"zz")
    for (fmt <- all_fmt) {
      val pos = new java.text.ParsePosition(0)
      val formatter = new java.text.SimpleDateFormat(fmt)
      // (Possibly we shouldn't do this?) This rejects nonstandardness, e.g.
      // out-of-range values such as month 13 or hour 25; that's useful for
      // error-checking in case someone messed up entering the date.
      formatter.setLenient(false)
      val canon_datestr = datestr.replace(":", "")
      val date = formatter.parse(canon_datestr, pos)
      if (date != null && pos.getIndex == canon_datestr.length)
        return Some(date.getTime)
    }
    None
  }

  /**
   * Parse a time offset specification, e.g. "5h" (or "+5h") for 5 hours,
   * or "3m2s" for "3 minutes 2 seconds".  Negative values are allowed,
   * to specify offsets going backwards in time.  If able to parse, return
   * a tuple (Some(millisecs),""); else return (None,errmess) specifying
   * an error message.
   */
  def parse_time_offset(str: String): (Option[Long], String) = {
    if (str.length == 0)
      return (None, "Time offset cannot be empty")
    val sections =
      split_with_delim(str.toLowerCase, "[a-z]+".r).filterNot {
        case (text, delim) => text.length == 0 && delim.length == 0 }
    val offset_secs =
      (for ((valstr, units) <- sections) yield {
        val multiplier =
          units match {
            case "s" => 1
            case "m" => 60
            case "h" => 60*60
            case "d" => 60*60*24
            case "w" => 60*60*24*7
            case "" => 
              return (None, "Missing units in component '%s' in time offset '%s'; should be e.g. '25s' or '10h30m'"
                format (valstr + units, str))
            case _ =>
              return (None, "Unrecognized component '%s' in time offset '%s'; should be e.g. '25s' or '10h30m'"
                format (valstr + units, str))
          }
        val value =
          try {
            valstr.toDouble
          } catch {
            case e: Exception => return (None,
              "Unable to convert value '%s' in component '%s' in time offset '%s': %s" format
              (valstr, valstr + units, str, e))
          }
        multiplier * value
      }).sum
    (Some((offset_secs * 1000).toLong), "")
  }

  /**
   * Parse a date and offset into an interval.  Should be of the
   * form TIME/LENGTH, e.g. '20100802180502PST/2h3m' (= starting at
   * August 2, 2010, 18:05:02 Pacific Standard Time, ending exactly
   * 2 hours 3 minutes later).  Negative offsets are allowed, to indicate
   * an interval backwards from a reference point.
   * 
   * @return Tuple of `(Some((start, end)),"")` if able to parse, else
   * return None along with an error message.
   */
  def parse_date_interval(str: String): (Option[(Long, Long)], String) = {
    val date_offset = str.split("/", -1)
    if (date_offset.length != 2)
      (None, "Time chunk %s must be of the format 'START/LENGTH'"
        format str)
    else {
      val Array(datestr, offsetstr) = date_offset
      val timelen = parse_time_offset(offsetstr) match {
        case (Some(len), "") => len
        case (None, errmess) => return (None, errmess)
        case _ => internal_error("Should never get here")
      }
      parse_date(datestr) match {
        case Some(date) =>
          (Some((date, date + timelen)), "")
        case None =>
          (None,
            "Can't parse time '%s'; should be something like 201008021805pm"
            format datestr)
      }
    }
  }

  // A 1-dimensional time coordinate (Epoch time, i.e. elapsed time since the
  // Jan 1, 1970 Unix Epoch, in milliseconds).  The use of a 64-bit long to
  // represent Epoch time in milliseconds is common in Java and also used in
  // Twitter (gives you over 300,000 years).  An alternative is to use a
  // double to represent seconds, which gets you approximately the same
  // accuracy -- 52 bits of mantissa to represent any integer <= 2^52
  // exactly, similar to the approximately 53 bits worth of seconds you
  // get when using milliseconds.
  //
  // Note that having this here is an important check on the correctness
  // of the code elsewhere -- if by mistake you leave off the type
  // parameters when calling a function, and the function asks for a type
  // with a serializer and there's only one such type available, Scala
  // automatically uses that one type.  Hence code may work fine until you
  // add a second serializable type, and then lots of compile errors.

  case class TimeCoord(millis: Long) {
    override def toString = "%s (%s)" format (millis, format_time(millis))
  }

  implicit object TimeCoord extends Serializer[TimeCoord] {
    def deserialize(foo: String) = TimeCoord(foo.toLong)
    def serialize(foo: TimeCoord) = "%s".format(foo.millis)
  }
}

package object time extends TimeImpl { }
