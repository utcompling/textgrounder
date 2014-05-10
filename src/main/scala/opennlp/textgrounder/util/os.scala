///////////////////////////////////////////////////////////////////////////////
//  os.scala
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

import scala.collection.mutable

import java.util.Date
import java.text.DateFormat
import java.io._

import print._
import text._
import time.format_minutes_seconds

package object os {

  ////////////////////////////////////////////////////////////////////////////
  //                             Resource Usage                             //
  ////////////////////////////////////////////////////////////////////////////

  /**
    * Return floating-point value, number of seconds since the Epoch
    **/
  def curtimesecs = System.currentTimeMillis/1000.0

  def curtimehuman = (new Date) toString

  def humandate_full(sectime: Double) =
    (new Date((sectime*1000).toLong)) toString
  def humandate_time(sectime: Double) =
    DateFormat.getTimeInstance.format((sectime*1000).toLong)

  import java.lang.management.ManagementFactory
  def getpid_str = ManagementFactory.getRuntimeMXBean.getName.split("@")(0)
  private var initialized = false
  private def check_initialized() {
    if (!initialized)
      throw new IllegalStateException("""You must call initialize_osutil()
at the beginning of your program, in order to use get_program_real_time_usage""")
  }
  /**
   * Call this if you use `get_program_real_time_usage` or `output_resource_usage`.
   * This is necessary in order to record the time at the beginning of the
   * program.
   */
  def initialize_osutil() {
    // Simply calling this function is enough, because it will trigger the
    // loading of the class associated with package object os, which
    // will cause `beginning_prog_time` to get set.  We set a flag to verify
    // that this is done.
    initialized = true
  }
  val beginning_prog_time = curtimesecs

  def get_program_real_time_usage = {
    check_initialized()
    curtimesecs - beginning_prog_time
  }

  /**
   * Wrap a function call, catching errors. If an error occurs, output the
   * error (prefixed with `errprefix`, a human-readable version of the
   * function called) and return `errval`. If the debug flag 'stack-trace'
   * (or 'stacktrace') is set, also output a stack trace. If the debug flag
   * 'no-catch' is set, don't catch errors (in this case, they will normally
   * terminate the program).
   */
  def wrap_call[Ret](fn: => Ret, errprefix: String, errval: Ret) = {
    if (debug.debug("no-catch"))
      fn
    else {
      try {
        fn
      } catch {
        case e: Exception => {
          if (debug.debug("stack-trace") || debug.debug("stacktrace")) {
            errprint(s"$errprefix:")
            e.printStackTrace
          } else
            errprint(s"$errprefix: $e")
          errval
        }
      }
    }
  }

  def get_program_cpu_time_usage_java = {
    ManagementFactory.getOperatingSystemMXBean.asInstanceOf[
      com.sun.management.OperatingSystemMXBean].
      getProcessCpuTime / 1000000000.0
  }

  // Get CPU time by running 'ps'. The following seems to work on both
  // Mac OS X and Linux, at least.
  def get_program_cpu_time_usage_ps(wraperr: Boolean = true): Double = {
    if (wraperr)
      return wrap_call(get_program_cpu_time_usage_ps(
        wraperr=false), "get_program_cpu_time_usage_ps", -1L)
    val header = "time"
    val input =
      io.capture_subprocess_output("ps", "-p", getpid_str, "-o", header)
    val lines = input.split('\n')
    for (line <- lines if line.trim != header.toUpperCase) {
      val min_sec = "^([0-9]+):([0-9.]+)$".r
      val hr_min_sec = "^([0-9]+):([0-9]+):([0-9.]+)$".r
      line.trim match {
        case min_sec(min, sec) => return min.toDouble*60 + sec.toDouble
        case hr_min_sec(hr, min, sec) =>
          return hr.toDouble*3600 + min.toDouble*60 + sec.toDouble
      }
    }
    return -1.0
  }

  /**
   * Return program CPU time usage. `method` determines how the CPU time
   *   is computed:
   *
   * "proc"       Use the /proc file system (not on Mac OS X)
   * "ps"         Call the 'ps' utility
   * "java"       Use Java built-in calls
   * "auto"       Try "java", then "proc", then "ps".
   */
  def get_program_cpu_time_usage(method: String = "auto"): (String, Double) = {
    method match {
      case "java" => (method, get_program_cpu_time_usage_java)
      // case "proc" => (method, get_program_cpu_usage_proc)
      case "ps" => (method, get_program_cpu_time_usage_ps())
      case "auto" => {
        val javatime = get_program_cpu_time_usage_java
        if (javatime >= 0) return ("java", javatime)
        // val proctime = get_program_cpu_time_usage_proc
        // if (proctime >= 0) return ("proc", proctime)
        val pstime = get_program_cpu_time_usage_ps()
        if (pstime >= 0) return ("ps", pstime)
        return ("unknown", -1.0)
      }
    }
  }

  /**
   * Return program memory usage. This is normally the resident-set size
   * (active-use memory) unless `virtual` is true, in which case it is
   * total virtual memory usage; however, if `method` = "java", the return
   * value is the Java heap size. (For Java apps, the virtual memory usage is
   * typically much more than the resident-set size, which is much more than
   * the heap size.) `method` determines how the memory usage is computed:
   *
   * "proc"       Use the /proc file system (not on Mac OS X)
   * "ps"         Call the 'ps' utility
   * "rusage"     Use a Java call that wraps getrusage() (not working)
   * "java"       Return Java heap size
   * "auto"       Try "proc", then "ps", then "rusage", then "java".
   */
  def get_program_memory_usage(virtual: Boolean = false,
      method: String = "auto"): (String, Long) = {
    method match {
      case "java" => (method, get_program_memory_usage_java())
      case "proc" => (method, get_program_memory_usage_proc(virtual = virtual))
      case "ps" => (method, get_program_memory_usage_ps(virtual = virtual))
      case "rusage" => (method, get_program_memory_usage_rusage())
      case "auto" => {
        val procmem = get_program_memory_usage_proc(virtual = virtual)
        if (procmem > 0) return ("proc", procmem)
        val psmem = get_program_memory_usage_ps(virtual = virtual)
        if (psmem > 0) return ("ps", psmem)
        val rusagemem = get_program_memory_usage_rusage()
        if (rusagemem > 0) return ("rusage", rusagemem)
        return ("java", get_program_memory_usage_java())
      }
    }
  }

  def get_program_memory_usage_java() = {
    System.gc()
    System.gc()
    val rt = Runtime.getRuntime
    rt.totalMemory - rt.freeMemory
  }

  def get_program_memory_usage_rusage() = {
    // val res = resource.getrusage(resource.RUSAGE_SELF)
    // // FIXME!  This is "maximum resident set size".  There are other more useful
    // // values, but on the Mac at least they show up as 0 in this structure.
    // // On Linux, alas, all values show up as 0 or garbage (e.g. negative).
    // res.ru_maxrss
    -1L
  }

  // Get memory usage by running 'ps'; getrusage() doesn't seem to work very
  // well.  The following seems to work on both Mac OS X and Linux, at least.
  def get_program_memory_usage_ps(virtual: Boolean = false,
      wraperr: Boolean = true): Long = {
    if (wraperr)
      return wrap_call(get_program_memory_usage_ps(
        virtual=virtual, wraperr=false), "get_program_memory_usage_ps", -1L)
    val header = if (virtual) "vsz" else "rss"
    val input =
      io.capture_subprocess_output("ps", "-p", getpid_str, "-o", header)
    val lines = input.split('\n')
    for (line <- lines if line.trim != header.toUpperCase)
      return 1024*line.trim.toLong
    return -1L
  }

  // Get memory usage by running 'proc'; this works on Linux and doesn't
  // require spawning a subprocess, which can crash when your program is
  // very large.
  def get_program_memory_usage_proc(virtual: Boolean = false,
      wraperr: Boolean = true): Long = {
    if (wraperr)
      return wrap_call(get_program_memory_usage_proc(
        virtual=virtual, wraperr=false), "get_program_memory_usage_proc", -1L)
    val header = if (virtual) "VmSize:" else "VmRSS:"
    if (!((new File("/proc/self/status")).exists))
      return -1L
    for (line <- io.localfh.openr("/proc/self/status")) {
        val trimline = line.trim
        if (trimline.startsWith(header)) {
          val size = ("""\s+""".r.split(trimline))(1).toLong
          return 1024*size
        }
      }
    return -1L
  }

  def format_bytes(bytes: Long) = {
    if (bytes >= 1000000000)
      "%.2f GB" format (bytes / 1000000000.0)
    else if (bytes >= 1000000)
      "%.2f MB" format (bytes / 1000000.0)
    else
      "%.2f KB" format (bytes / 1000.0)
  }

  def output_memory_usage(virtual: Boolean = false) {
    for (method <- List("auto", "java", "proc", "ps", "rusage")) {
      val (meth, mem) =
        get_program_memory_usage(virtual = virtual, method = method)
      val memtype = if (virtual) "virtual size" else "resident set size"
      val methstr = if (method == "auto") "auto=%s" format meth else method
      errout("Memory usage, %s (%s): ", memtype, methstr)
      if (mem <= 0)
        errprint("Unknown")
      else
        errprint("%s", format_bytes(mem))
    }
  }

  def output_resource_usage(dojava: Boolean = true,
      omit_time: Boolean = false) {
    if (!omit_time) {
      errprint("Total elapsed time since program start: %s",
        format_minutes_seconds(get_program_real_time_usage))
      val (_, cputime) = get_program_cpu_time_usage()
      errprint("Total CPU time since program start: %s",
        format_minutes_seconds(cputime))
      // To compare Java and ps to make sure they're the same:
      // errprint("Total CPU time since program start (Java): %s",
      //   format_minutes_seconds(get_program_cpu_time_usage_java))
      // errprint("Total CPU time since program start (ps): %s",
      //   format_minutes_seconds(get_program_cpu_time_usage_ps()))
    }
    val (vszmeth, vsz) = get_program_memory_usage(virtual = true,
      method = "auto")
    val (rssmeth, rss) = get_program_memory_usage(virtual = false,
      method = "auto")
    // Goes a bit over 80 chars this way
    //val memstr = "virtual (%s): %s, resident (%s): %s" format (
    //  vszmeth, format_bytes(vsz), rssmeth, format_bytes(rss))
    val memstr = "virtual: %s, resident: %s" format (
      format_bytes(vsz), format_bytes(rss))
    if (dojava) {
      val (_, java) = get_program_memory_usage(virtual = false,
        method = "java")
      errprint("Memory usage: %s, Java heap: %s", memstr, format_bytes(java))
    } else {
      errprint("Memory usage: %s", memstr)
      System.gc()
    }
  }

  /* For testing the output_memory_usage() function. */
  object TestMemUsage extends App {
    output_memory_usage()
  }
}

