///////////////////////////////////////////////////////////////////////////////
//  print.scala
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

import scala.util.control.Breaks._
import scala.collection.mutable

// The following says to import everything except java.io.Console, because
// it conflicts with (and overrides) built-in scala.Console. (Technically,
// it imports everything but in the process aliases Console to _, which
// has the effect of making it inaccessible. _ is special in Scala and has
// various meanings.)
import java.io.{Console=>_,_}

import text._
import io._
import os._

////////////////////////////////////////////////////////////////////////////
//                            Text output functions                       //
////////////////////////////////////////////////////////////////////////////

protected class PrintPackage {

  // This stuff sucks.  Need to create new Print streams to get the expected
  // UTF-8 output, since the existing System.out/System.err streams don't do it!
  val stdout_stream = new PrintStream(System.out, /* autoFlush */ true,
    "UTF-8")
  val stderr_stream = new PrintStream(System.err, /* autoFlush */ true,
    "UTF-8")

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

  def errfmt(format: String, args: Any*) = {
    // If no arguments, assume that we've been passed a raw string to print,
    // so print it directly rather than passing it to 'format', which might
    // munge % signs
    import scala.runtime.ScalaRunTime.stringOf
    if (args.length == 0) format
    else {
      val strargs = args.map { x =>
        x match {
          case null => stringOf(x)
          case _: Boolean => x
          case _: Byte => x
          case _: Char => x
          case _: Short => x
          case _: Int => x
          case _: Long => x
          case _: Float => x
          case _: Double => x
          case _: BigInt => x
          case _: BigDecimal => x
          case _: String => x
          case _ => stringOf(x)
        }
      }
      format format (strargs: _*)
    }
  }

  def format_outtext(format: String, args: Any*) = {
    val outtext = errfmt(format, args: _*)
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

  def errln(format: String, args: Any*) {
    errprint(format, args: _*)
  }

  def errprint(format: String, args: Any*) {
    val text = format_outtext(format, args: _*)
    errout_stream.println(text)
    need_prefix = true
    errout_stream.flush()
  }

  def errout(format: String, args: Any*) {
    val text = format_outtext(format, args: _*)
    errout_stream.print(text)
    need_prefix = text.last == '\n'
    errout_stream.flush()
  }

  def outprint(format: String, args: Any*) {
    uniprint(format_outtext(format, args: _*))
  }

  def outout(format: String, args: Any*) {
    uniout(format_outtext(format, args: _*))
  }
}

  ////////////////////////////////////////////////////////////////////////////
  //                              Table Output                              //
  ////////////////////////////////////////////////////////////////////////////

package object print extends PrintPackage { }

