///////////////////////////////////////////////////////////////////////////////
//  error.scala
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

import io._
import os._
import print._

protected class ErrorPackage {
  /**
    * Output a warning, formatting into UTF-8 as necessary.
    */
  def warning(format: String, args: Any*) {
    errprint("Warning: " + format, args: _*)
  }
  
  private object WarningsSeen {
    val warnings_seen = mutable.Set[String]()
  }

  /**
    * Output a warning, formatting into UTF-8 as necessary.
    * But don't output if already seen.
    */
  def warning_once(format: String, args: Any*) {
    val warnstr = format_outtext("Warning: " + format, args: _*)
    if (!(WarningsSeen.warnings_seen contains warnstr)) {
      WarningsSeen.warnings_seen += warnstr
      errprint(warnstr)
    }
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
  
  /**
   * Return the stack trace of an exception as a string.
   */
  def stack_trace_as_string(e: Exception) = {
    val writer = new StringWriter()
    val pwriter = new PrintWriter(writer)
    e.printStackTrace(pwriter)
    pwriter.close()
    writer.toString
  }

  class RethrowableRuntimeException(
    message: String,
    cause: Option[Throwable] = None
  ) extends RuntimeException(message) {
    if (cause != None)
      initCause(cause.get)

    /**
     * Alternate constructor.
     *
     * @param message  exception message
     */
    def this(msg: String) = this(msg, None)

    /**
     * Alternate constructor.
     *
     * @param message  exception message
     * @param cause    wrapped, or nested, exception
     */
    def this(msg: String, cause: Throwable) = this(msg, Some(cause))
  }

  /**
   * An exception thrown to indicate an internal error (program gets to a
   * state it should never reach, similar to an assertion failure).
   */
  case class InternalError(
    message: String,
    cause: Option[Throwable] = None
  ) extends RethrowableRuntimeException(message, cause)

  /**
   * An exception thrown to indicate that a part of the code that
   * isn't implemented yet, but should be.
   */
  case class FixmeError(
    message: String,
    cause: Option[Throwable] = None
  ) extends RethrowableRuntimeException(message, cause)

  /**
   * Signal an internal error (program gets to a state it should never reach,
   * similar to an assertion failure).
   */
  def internal_error(message: String) =
    throw new InternalError(message)

  /**
   * Signal an error due to a part of the code that isn't implemented yet,
   * but should be.
   */
  def fixme_error(message: String) =
    throw new FixmeError(message)

  /**
   * Signal an error due to attempting an operation that isn't supported
   * and will never be.
   */
  def unsupported(message: String = "") =
    throw new UnsupportedOperationException(message)
}

package object error extends ErrorPackage { }

