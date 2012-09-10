///////////////////////////////////////////////////////////////////////////////
//  osutil.scala
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

import scala.collection.mutable

import java.util.Date
import java.text.DateFormat
import java.io._

import ioutil._
import printutil._
import textutil._
import timeutil.format_minutes_seconds

package object osutil {

  ////////////////////////////////////////////////////////////////////////////
  //                             Resource Usage                             //
  ////////////////////////////////////////////////////////////////////////////

  /**
    * Return floating-point value, number of seconds since the Epoch
    **/
  def curtimesecs() = System.currentTimeMillis/1000.0

  def curtimehuman() = (new Date()) toString

  def humandate_full(sectime: Double) =
    (new Date((sectime*1000).toLong)) toString
  def humandate_time(sectime: Double) =
    DateFormat.getTimeInstance().format((sectime*1000).toLong)

  import java.lang.management._
  def getpid() = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)
  private var initialized = false
  private def check_initialized() {
    if (!initialized)
      throw new IllegalStateException("""You must call initialize_osutil() 
at the beginning of your program, in order to use get_program_time_usage()""")
  }
  /**
   * Call this if you use `get_program_time_usage` or `output_resource_usage`.
   * This is necessary in order to record the time at the beginning of the
   * program.
   */
  def initialize_osutil() {
    // Simply calling this function is enough, because it will trigger the
    // loading of the class associated with package object osutil, which
    // will cause `beginning_prog_time` to get set.  We set a flag to verify
    // that this is done.
    initialized = true
  }
  val beginning_prog_time = curtimesecs()
  
  def get_program_time_usage() = {
    check_initialized()
    curtimesecs() - beginning_prog_time
  }

  /**
   * Return memory usage as a 
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
  
  def wrap_call[Ret](fn: => Ret, errval: Ret) = {
    try {
      fn
    } catch {
      case e@_ => { errprint("%s", e); errval }
    }
  }

  // Get memory usage by running 'ps'; getrusage() doesn't seem to work very
  // well.  The following seems to work on both Mac OS X and Linux, at least.
  def get_program_memory_usage_ps(virtual: Boolean = false,
      wraperr: Boolean = true): Long = {
    if (wraperr)
      return wrap_call(get_program_memory_usage_ps(
        virtual=virtual, wraperr=false), -1L)
    val header = if (virtual) "vsz" else "rss"
    val pid = getpid()
    val input =
      capture_subprocess_output("ps", "-p", pid.toString, "-o", header)
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
        virtual=virtual, wraperr=false), -1L)
    val header = if (virtual) "VmSize:" else "VmRSS:"
    if (!((new File("/proc/self/status")).exists))
      return -1L
    for (line <- local_file_handler.openr("/proc/self/status")) {
        val trimline = line.trim
        if (trimline.startsWith(header)) {
          val size = ("""\s+""".r.split(trimline))(1).toLong
          return 1024*size
        }
      }
    return -1L
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
        errprint("%s bytes", with_commas(mem))
    }
  }

  def output_resource_usage(dojava: Boolean = true) {
    errprint("Total elapsed time since program start: %s",
             format_minutes_seconds(get_program_time_usage()))
    val (vszmeth, vsz) = get_program_memory_usage(virtual = true,
      method = "auto")
    errprint("Memory usage, virtual memory size (%s): %s bytes", vszmeth,
      with_commas(vsz))
    val (rssmeth, rss) = get_program_memory_usage(virtual = false,
      method = "auto")
    errprint("Memory usage, actual (i.e. resident set) (%s): %s bytes", rssmeth,
      with_commas(rss))
    if (dojava) {
      val (_, java) = get_program_memory_usage(virtual = false,
        method = "java")
      errprint("Memory usage, Java heap: %s bytes", with_commas(java))
    } else
      System.gc()
  }

  /* For testing the output_memory_usage() function. */
  object TestMemUsage extends App {
    output_memory_usage()
  }
}

