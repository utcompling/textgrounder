///////////////////////////////////////////////////////////////////////////////
//  printutil.scala
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
import scala.collection.mutable

// The following says to import everything except java.io.Console, because
// it conflicts with (and overrides) built-in scala.Console. (Technically,
// it imports everything but in the process aliases Console to _, which
// has the effect of making it inaccessible. _ is special in Scala and has
// various meanings.)
import java.io.{Console=>_,_}

import textutil._
import ioutil._
import osutil._

package object printutil {

  ////////////////////////////////////////////////////////////////////////////
  //                            Text output functions                       //
  ////////////////////////////////////////////////////////////////////////////

  // This stuff sucks.  Need to create new Print streams to get the expected
  // UTF-8 output, since the existing System.out/System.err streams don't do it!
  val stdout_stream = new PrintStream(System.out, true, "UTF-8") 
  val stderr_stream = new PrintStream(System.err, true, "UTF-8") 

  /**
    Set Java System.out and System.err, and Scala Console.out and Console.err,
    so that they convert text to UTF-8 upon output (rather than e.g. MacRoman,
    the default on Mac OS X).
   */
  def set_stdout_stderr_utf_8() {
    // Fuck me to hell, have to fix things up in a non-obvious way to
    // get UTF-8 output on the Mac (default is MacRoman???).
    System.setOut(stdout_stream)
    System.setErr(stderr_stream)
    Console.setOut(System.out)
    Console.setErr(System.err)
  }

  def uniprint(text: String, outfile: PrintStream = System.out) {
    outfile.println(text)
  }
  def uniout(text: String, outfile: PrintStream = System.out) {
    outfile.print(text)
  }

  var errout_prefix = ""

  def set_errout_prefix(prefix: String) {
    errout_prefix = prefix
  }
 
  var need_prefix = true

  var errout_stream: PrintStream = System.err

  def set_errout_stream(stream: PrintStream) {
    if (stream == null)
      errout_stream = System.err
    else
      errout_stream = stream
  }

  def get_errout_stream(file: String) = {
    if (file == null)
      System.err
    else
      (new LocalFileHandler).openw(file, append = true, bufsize = -1)
  }

  def set_errout_file(file: String) {
    set_errout_stream(get_errout_stream(file))
  }

  protected def format_outtext(format: String, args: Any*) = {
    // If no arguments, assume that we've been passed a raw string to print,
    // so print it directly rather than passing it to 'format', which might
    // munge % signs
    val outtext =
      if (args.length == 0) format
      else format format (args: _*)
    if (need_prefix)
      errout_prefix + outtext
    else
      outtext
  }

  def errfile(file: String, format: String, args: Any*) {
    val stream = get_errout_stream(file)
    stream.println(format_outtext(format, args: _*))
    need_prefix = true
    stream.flush()
    if (stream != System.err)
      stream.close()
  }

  def errprint(format: String, args: Any*) {
    errout_stream.println(format_outtext(format, args: _*))
    need_prefix = true
    errout_stream.flush()
  }

  def errout(format: String, args: Any*) {
    val text = format_outtext(format, args: _*)
    errout_stream.print(text)
    need_prefix = text.last == '\n'
    errout_stream.flush()
  }

  /**
    Output a warning, formatting into UTF-8 as necessary.
    */
  def warning(format: String, args: Any*) {
    errprint("Warning: " + format, args: _*)
  }
  
  /**
    Output a value, for debugging through print statements.
    Basically same as just caling errprint() or println() or whatever,
    but useful because the call to debprint() more clearly identifies a
    temporary piece of debugging code that should be removed when the
    bug has been identified.
   */
  def debprint(format: String, args: Any*) {
    errprint("Debug: " + format, args: _*)
  }
  
  def print_msg_heading(msg: String, blank_lines_before: Int = 1) {
    for (x <- 0 until blank_lines_before)
      errprint("")
    errprint(msg)
    errprint("-" * msg.length)
  }

  ////////////////////////////////////////////////////////////////////////////
  //                              Table Output                              //
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Given a list of tuples, output the list, one line per tuple.
   *
   * @param outfile If specified, send output to this stream instead of
   *   stdout.
   * @param indent If specified, indent all rows by this string (usually
   *   some number of spaces).
   * @param maxrows If specified, output at most this many rows.
   */
  def output_tuple_list[T,U](
      items: Seq[(T,U)], outfile: PrintStream = System.out,
      indent: String = "", maxrows: Int = -1) {
    var its = items
    if (maxrows >= 0)
      its = its.slice(0, maxrows)
    for ((key, value) <- its)
      outfile.println("%s%s = %s" format (indent, key, value))
  }

  /**
   * Given a list of tuples, where the second element of the tuple is a
   * number and the first a key, output the list, sorted on the numbers from
   * bigger to smaller.  Within a given number, normally sort the items
   * alphabetically.
   *
   * @param, keep_secondary_order If true, the original order of items is
   *   left instead of sorting secondarily.
   * @param outfile If specified, send output to this stream instead of
   *   stdout.
   * @param indent If specified, indent all rows by this string (usually
   *   some number of spaces).
   * @param maxrows If specified, output at most this many rows.
   */
  def output_reverse_sorted_list[T <% Ordered[T],U <% Ordered[U]](
      items: Seq[(T,U)], keep_secondary_order: Boolean = false,
      outfile: PrintStream = System.out, indent: String = "",
      maxrows: Int = -1) {
    var its = items
    if (!keep_secondary_order)
      its = its sortBy (_._1)
    its = its sortWith (_._2 > _._2)
    output_tuple_list(its, outfile, indent, maxrows)
  }
  
  /**
   * Given a table with values that are numbers, output the table, sorted on
   * the numbers from bigger to smaller.  Within a given number, normally
   * sort the items alphabetically.
   *
   * @param, keep_secondary_order If true, the original order of items is
   *   left instead of sorting secondarily.
   * @param outfile If specified, send output to this stream instead of
   *   stdout.
   * @param indent If specified, indent all rows by this string (usually
   *   some number of spaces).
   * @param maxrows If specified, output at most this many rows.
   */
  def output_reverse_sorted_table[T <% Ordered[T],U <% Ordered[U]](
      table: collection.Map[T,U], keep_secondary_order: Boolean = false,
      outfile: PrintStream = System.out, indent: String = "",
      maxrows: Int = -1) {
    output_reverse_sorted_list(table toList, keep_secondary_order,
      outfile, indent, maxrows)
  }

  /**
   * Output a table, sorted by its key.
   *
   * @param outfile If specified, send output to this stream instead of
   *   stdout.
   * @param indent If specified, indent all rows by this string (usually
   *   some number of spaces).
   * @param maxrows If specified, output at most this many rows.
   */
  def output_key_sorted_table[T <% Ordered[T],U](
      table: collection.Map[T,U],
      outfile: PrintStream = System.out, indent: String = "",
      maxrows: Int = -1) {
    output_tuple_list(table.toSeq.sortBy (_._1), outfile, indent,
      maxrows)
  }
}
