///////////////////////////////////////////////////////////////////////////////
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

  def errprint(format: String, args: Any*) {
    System.err.println(format_outtext(format, args: _*))
    need_prefix = true
    System.err.flush()
  }

  def errout(format: String, args: Any*) {
    val text = format_outtext(format, args: _*)
    System.err.print(text)
    need_prefix = text.last == '\n'
    System.err.flush()
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
  
  ////////////////////////////////////////////////////////////////////////////
  //                              Table Output                              //
  ////////////////////////////////////////////////////////////////////////////

  // Given a list of tuples, where the second element of the tuple is a number and
  // the first a key, output the list, sorted on the numbers from bigger to
  // smaller.  Within a given number, sort the items alphabetically, unless
  // keep_secondary_order is true, in which case the original order of items is
  // left.  If 'outfile' is specified, send output to this stream instead of
  // stdout.  If 'indent' is specified, indent all rows by this string (usually
  // some number of spaces).  If 'maxrows' is specified, output at most this many
  // rows.
  def output_reverse_sorted_list[T <% Ordered[T],U <% Ordered[U]](
      items: Seq[(T,U)],
      outfile: PrintStream=System.out, indent: String="",
      keep_secondary_order: Boolean=false, maxrows: Int = -1) {
    var its = items
    if (!keep_secondary_order)
      its = its sortBy (_._1)
    its = its sortWith (_._2 > _._2)
    if (maxrows >= 0)
      its = its.slice(0, maxrows)
    for ((key, value) <- its)
      outfile.println("%s%s = %s" format (indent, key, value))
  }
  
  // Given a table with values that are numbers, output the table, sorted
  // on the numbers from bigger to smaller.  Within a given number, sort the
  // items alphabetically, unless keep_secondary_order is true, in which case
  // the original order of items is left.  If 'outfile' is specified, send
  // output to this stream instead of stdout.  If 'indent' is specified, indent
  // all rows by this string (usually some number of spaces).  If 'maxrows'
  // is specified, output at most this many rows.
  def output_reverse_sorted_table[T <% Ordered[T],U <% Ordered[U]](
      table: collection.Map[T,U],
      outfile: PrintStream=System.out, indent: String="",
      keep_secondary_order: Boolean=false, maxrows: Int = -1) {
    output_reverse_sorted_list(table toList, outfile, indent,
      keep_secondary_order, maxrows)
  }
}
