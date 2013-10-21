///////////////////////////////////////////////////////////////////////////////
//  io.scala
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
import java.util.NoSuchElementException

import org.apache.commons.compress.compressors.bzip2._
import org.apache.commons.compress.compressors.gzip._

import print._
import text._
import os._

/**
 * A 'package object' declaration creates a new subpackage and puts the
 * stuff here directly in package scope.  This makes it possible to have
 * functions in package scope instead of inside a class or object (i.e.
 * singleton class).  The functions here are accessed using 'import util.io._'
 * outside of package 'util', and simply 'import io._' inside of it.
 */

package io {

  //////////////////////////////////////////////////////////////////////////////
  //                            File reading functions                        //
  //////////////////////////////////////////////////////////////////////////////
  
  case class FileFormatException(
    message: String,
    cause: Option[Throwable] = None
  ) extends RethrowableRuntimeException(message, cause)

  /**
   * Iterator that yields lines in a given encoding (by default, UTF-8) from
   * an input stream, usually with any terminating newline removed and usually
   * with automatic closing of the stream when EOF is reached. `close` can be
   * called at any point to force the iterator to not return any more lines.
   *
   * @param stream Input stream to read from.
   * @param encoding Encoding of the text; by default, UTF-8.
   * @param chomp If true (the default), remove any terminating newline.
   *   Any of LF, CRLF or CR will be removed at end of line.
   * @param close If true (the default), automatically close the stream when
   *   EOF is reached.
   * @param errors How to handle conversion errors. (FIXME: Not implemented.)
   */
  class FileIterator(
      stream: InputStream,
      encoding: String = "UTF-8",
      chomp: Boolean = true,
      close: Boolean = true,
      errors: String = "strict"
  ) extends Iterator[String] {
    var ireader = new InputStreamReader(stream, encoding)
    var reader =
      // Wrapping in a BufferedReader is necessary because readLine() doesn't
      // exist on plain InputStreamReaders
      /* if (bufsize > 0) new BufferedReader(ireader, bufsize) else */
      new BufferedReader(ireader)
    var nextline: String = null
    var hit_eof: Boolean = false
    protected def getNextLine() = {
      if (hit_eof) false
      else {
        nextline = reader.readLine()
        // errprint("Read line: %s" format nextline)
        if (nextline == null) {
          hit_eof = true
          if (close)
            close()
          false
        } else {
          if (chomp) {
            if (nextline.endsWith("\r\n"))
              nextline = nextline.dropRight(2)
            else if (nextline.endsWith("\r"))
              nextline = nextline.dropRight(1)
            else if (nextline.endsWith("\n"))
              nextline = nextline.dropRight(1)
          }
          true
        }
      }
    }

    def hasNext = {
      if (nextline != null) true
      else if (reader == null) false
      else getNextLine()
    }

    def next() = {
      if (!hasNext) Iterator.empty.next
      else {
        val ret = nextline
        nextline = null
        ret
      }
    }

    def close() {
      hit_eof = true
      nextline = null
      if (reader != null) {
        reader.close()
        reader = null
      }
    }
  }

  abstract class FileHandler {
    /**
     * Return an InputStream that reads from the given file, usually with
     * buffering.
     *
     * @param filename Name of the file.
     * @param bufsize Buffering size.  If 0 (the default), the default
     *   buffer size is used.  If &gt; 0, the specified size is used.  If
     *   &lt; 0, there is no buffering.
     */
    def get_input_stream(filename: String, bufsize: Int = 0) = {
      val raw_in = get_raw_input_stream(filename)
      if (bufsize < 0)
        raw_in
      else if (bufsize == 0)
        new BufferedInputStream(raw_in)
      else
        new BufferedInputStream(raw_in, bufsize)
    }

    /**
     * Return an OutputStream that writes to the given file, usually with
     * buffering.
     *
     * @param filename Name of the file.
     * @param bufsize Buffering size.  If 0 (the default), the default
     *   buffer size is used.  If &gt; 0, the specified size is used.  If
     *   &lt; 0, there is no buffering.
     */
    def get_output_stream(filename: String, append: Boolean,
        bufsize: Int = 0) = {
      val raw_out = get_raw_output_stream(filename, append)
      if (bufsize < 0)
        raw_out
      else if (bufsize == 0)
        new BufferedOutputStream(raw_out)
      else
        new BufferedOutputStream(raw_out, bufsize)
    }

    /**
     * Open a filename with the given encoding (by default, UTF-8) and
     * optional decompression (by default, based on the filename), and
     * return an iterator that yields lines, usually with any terminating
     * newline removed and usually with automatic closing of the stream
     * when EOF is reached.
     *
     * @param filename Name of file to read from.
     * @param encoding Encoding of the text; by default, UTF-8.
     * @param compression Compression of the file (by default, "byname").
     *   Valid values are "none" (no compression), "byname" (use the
     *   extension of the filename to determine the compression), "gzip"
     *   and "bzip2".
     * @param chomp If true (the default), remove any terminating newline.
     *   Any of LF, CRLF or CR will be removed at end of line.
     * @param close If true (the default), automatically close the stream when
     *   EOF is reached.
     * @param errors How to handle conversion errors. (FIXME: Not implemented.)
     * @param bufsize Buffering size.  If 0 (the default), the default
     *   buffer size is used.  If &gt; 0, the specified size is used.  If
     *   &lt; 0, there is no buffering.
     *
     * @return A tuple `(iterator, compression_type, uncompressed_filename)`
     *   where `iterator` is the iterator over lines, `compression_type` is
     *   a string indicating the actual compression of the file ("none",
     *   "gzip" or "bzip2") and `uncompressed_filename` is the name the
     *   uncompressed file would have (typically by removing the extension
     *   that indicates compression).
     *
     * @see `FileIterator`, `get_input_stream`,
     *   `get_input_stream_handling_compression`
     */
    def openr_with_compression_info(filename: String,
        encoding: String = "UTF-8", compression: String = "byname",
        chomp: Boolean = true, close: Boolean = true,
        errors: String = "strict", bufsize: Int = 0) = {
      val (stream, comtype, realname) =
        get_input_stream_handling_compression(filename,
          compression=compression, bufsize=bufsize)
      (new FileIterator(stream, encoding=encoding, chomp=chomp, close=close,
         errors=errors), comtype, realname)
    }
    
    /**
     * Open a filename with the given encoding (by default, UTF-8) and
     * optional decompression (by default, based on the filename), and
     * return an iterator that yields lines, usually with any terminating
     * newline removed and usually with automatic closing of the stream
     * when EOF is reached.
     *
     * @param filename Name of file to read from.
     * @param encoding Encoding of the text; by default, UTF-8.
     * @param compression Compression of the file (by default, "byname").
     *   Valid values are "none" (no compression), "byname" (use the
     *   extension of the filename to determine the compression), "gzip"
     *   and "bzip2".
     * @param chomp If true (the default), remove any terminating newline.
     *   Any of LF, CRLF or CR will be removed at end of line.
     * @param close If true (the default), automatically close the stream when
     *   EOF is reached.
     * @param errors How to handle conversion errors. (FIXME: Not implemented.)
     * @param bufsize Buffering size.  If 0 (the default), the default
     *   buffer size is used.  If &gt; 0, the specified size is used.  If
     *   &lt; 0, there is no buffering.
     *
     * @return An iterator over lines.  Use `openr_with_compression_info` to
     *   also get the actual type of compression and the uncompressed name
     *   of the file (minus any extension like .gz or .bzip2). The iterator
     *   will close itself automatically when EOF is reached if the `close`
     *   option is set to true (the default), or it can be closed explicitly
     *   using the `close()` method on the iterator.
     *
     * @see `FileIterator`, `openr_with_compression_info`, `get_input_stream`,
     *   `get_input_stream_handling_compression`
     */
    def openr(filename: String, encoding: String = "UTF-8",
        compression: String = "byname", chomp: Boolean = true,
        close: Boolean = true, errors: String = "strict", bufsize: Int = 0) = {
      val (iterator, _, _) = openr_with_compression_info(filename,
        encoding=encoding, compression=compression, chomp=chomp, close=close,
        errors=errors, bufsize=bufsize)
      iterator
    }
    
    /**
     * Wrap an InputStream with optional decompression.  It is strongly
     * recommended that the InputStream be buffered.
     *
     * @param stream Input stream.
     * @param compression Compression type.  Valid values are "none" (no
     *   compression), "gzip", and "bzip2".
     */
    def wrap_input_stream_with_compression(stream: InputStream,
        compression: String) = {
      if (compression == "none") stream
      else if (compression == "gzip") new GzipCompressorInputStream(stream)
      else if (compression == "bzip2") new BZip2CompressorInputStream(stream)
      else throw new IllegalArgumentException(
        "Invalid compression argument: %s" format compression)
    }

    /**
     * Wrap an OutputStream with optional compression.  It is strongly
     * recommended that the OutputStream be buffered.
     *
     * @param stream Output stream.
     * @param compression Compression type.  Valid values are "none" (no
     *   compression), "gzip", and "bzip2".
     */
    def wrap_output_stream_with_compression(stream: OutputStream,
        compression: String) = {
      if (compression == "none") stream
      else if (compression == "gzip") new GzipCompressorOutputStream(stream)
      else if (compression == "bzip2") new BZip2CompressorOutputStream(stream)
      else throw new IllegalArgumentException(
        "Invalid compression argument: %s" format compression)
    }

    /**
     * Create an InputStream that reads from the given file, usually with
     * buffering and automatic decompression.  Either the decompression
     * format can be given explicitly (including "none"), or the function can
     * be instructed to use the extension of the filename to determine the
     * compression format (e.g. ".gz" for gzip).
     *
     * @param filename Name of the file.
     * @param compression Compression of the file (by default, "byname").
     *   Valid values are "none" (no compression), "byname" (use the
     *   extension of the filename to determine the compression), "gzip"
     *   and "bzip2".
     * @param bufsize Buffering size.  If 0 (the default), the default
     *   buffer size is used.  If &gt; 0, the specified size is used.  If
     *   &lt; 0, there is no buffering.
     *
     * @return A tuple `(stream, compression_type, uncompressed_filename)`
     *   where `stream` is the stream to read from, `compression_type` is
     *   a string indicating the actual compression of the file ("none",
     *   "gzip" or "bzip2") and `uncompressed_filename` is the name the
     *   uncompressed file would have (typically by removing the extension
     *   that indicates compression).
     */
    def get_input_stream_handling_compression(filename: String,
        compression: String = "byname", bufsize: Int = 0) = {
      val raw_in = get_input_stream(filename, bufsize)
      val comtype =
        if (compression == "byname") {
          if (BZip2Utils.isCompressedFilename(filename)) "bzip2"
          else if (GzipUtils.isCompressedFilename(filename)) "gzip"
          else "none"
        } else compression
      val in = wrap_input_stream_with_compression(raw_in, comtype)
      val realname = comtype match {
        case "gzip" => GzipUtils.getUncompressedFilename(filename)
        case "bzip2" => BZip2Utils.getUncompressedFilename(filename)
        case _ => {
          assert(comtype == "none",
            "wrap_input_stream_with_compression should have verified value")
          filename
        }
      }
      (in, comtype, realname)
    }

    /**
     * Create an OutputStream that writes ito the given file, usually with
     * buffering and automatic decompression.
     *
     * @param filename Name of the file.  The filename will automatically
     *   have a suffix added to it to indicate compression, if compression
     *   is called for.
     * @param compression Compression of the file (by default, "none").
     *   Valid values are "none" (no compression), "gzip" and "bzip2".
     * @param bufsize Buffering size.  If 0 (the default), the default
     *   buffer size is used.  If &gt; 0, the specified size is used.  If
     *   &lt; 0, there is no buffering.
     *
     * @return A tuple `(stream, compressed_filename)`, `stream` is the
     *   stream to write to and `compressed_filename` is the actual name
     *   assigned to the file, including the compression suffix, if any.
     */
    def get_output_stream_handling_compression(filename: String,
        append: Boolean, compression: String = "none", bufsize: Int = 0) = {
      val realname = compression match {
        case "gzip" => GzipUtils.getCompressedFilename(filename)
        case "bzip2" => BZip2Utils.getCompressedFilename(filename)
        case "none" => filename
        case _ => throw new IllegalArgumentException(
          "Invalid compression argument: %s" format compression)
        }
      val raw_out = get_output_stream(realname, append, bufsize)
      val out = wrap_output_stream_with_compression(raw_out, compression)
      (out, realname)
    }

    /**
     * Open a file for writing, with optional compression (by default, no
     * compression), and encoding (by default, UTF-8) and return a
     * PrintStream that will write to the file.
     *
     * @param filename Name of file to write to.  The filename will
     *   automatically have a suffix added to it to indicate compression,
     *   if compression is called for.
     * @param encoding Encoding of the text; by default, UTF-8.
     * @param compression Compression type.  Valid values are "none" (no
     *   compression), "gzip", and "bzip2".
     * @param bufsize Buffering size.  If 0 (the default), the default
     *   buffer size is used.  If &gt; 0, the specified size is used.  If
     *   &lt; 0, there is no buffering.
     * @param autoflush If true, automatically flush the PrintStream after
     *   every output call. (Note that if compression is in effect, the
     *   flush may not actually cause anything to get written.)
     */
    def openw(filename: String, append: Boolean = false,
        encoding: String = "UTF-8", compression: String = "none",
        bufsize: Int = 0, autoflush: Boolean = false) = {
      val (out, _) =
        get_output_stream_handling_compression(filename, append, compression,
          bufsize)
      new PrintStream(out, autoflush, encoding)
    }

    /* ----------- Abstract functions below this line ----------- */

    /**
     * Return an unbuffered InputStream that reads from the given file.
     */
    def get_raw_input_stream(filename: String): InputStream

    /**
     * Return an unbuffered OutputStream that writes to the given file,
     * overwriting an existing file.
     */
    def get_raw_output_stream(filename: String, append: Boolean): OutputStream
    /**
     * Split a string naming a file into the directory it's in and the
     * final component.
     */
    def split_filename(filename: String): (String, String)
    /**
     * Join a string naming a directory to a string naming a file.  If the
     * file is relative, it is to be interpreted relative to the directory.
     */
    def join_filename(dir: String, file: String): String
    /**
     * Is this file a directory?
     */
    def is_directory(filename: String): Boolean
    /**
     * Create a directory, along with any missing parents.  Returns true
     * if the directory was created, false if it already exists.
     */
    def make_directories(filename: String): Boolean
    /**
     * List the files in the given directory.
     */
    def list_files(dir: String): Iterable[String]

  }

  class LocalFileHandler extends FileHandler {
    def check_exists(filename: String) {
      if (!new File(filename).exists)
        throw new FileNotFoundException("%s (No such file or directory)"
          format filename)
    }
    def get_raw_input_stream(filename: String) = new FileInputStream(filename)
    def get_raw_output_stream(filename: String, append: Boolean) =
      new FileOutputStream(filename, append)
    def split_filename(filename: String) = {
      val file = new File(filename)
      (file.getParent, file.getName)
    }
    def join_filename(dir: String, file: String) =
      new File(dir, file).toString
    def is_directory(filename: String) = {
      check_exists(filename)
      new File(filename).isDirectory
    }
    def make_directories(filename: String): Boolean =
      new File(filename).mkdirs
    def list_files(dir: String) = {
      check_exists(dir)
      for (file <- new File(dir).listFiles)
        yield file.toString
    }
  }

  class StdFileHandler extends FileHandler {
    def not_found(filename: String) = {
      throw new FileNotFoundException("%s (No such file or directory)"
        format filename)
    }
    def get_raw_input_stream(filename: String) =
      if (filename == "stdin") System.in
      else not_found(filename)
    def get_raw_output_stream(filename: String, append: Boolean) =
      filename match {
        case "stdout" => System.out
        case "stderr" => System.err
        case _ => not_found(filename)
      }
    def split_filename(filename: String) = unsupported()
    def join_filename(dir: String, file: String) = unsupported()
    def is_directory(filename: String) = unsupported()
    def make_directories(filename: String) = unsupported()
    def list_files(dir: String) = unsupported()
  }
}

package object io {
  /**
   * A file handler for the local file system. Use this to operate on
   * "normal" (local) files.
   */
  val localfh = new LocalFileHandler

  /**
   * A file handler for stdio handles (stdin, stdout, stderr).
   */
  val stdfh = new StdFileHandler

  /**
   * Iterate over the given files, recursively processing the files in
   * each directory given.
   */
  def iter_files_recursively(filehand: FileHandler,
      files: Iterable[String]): Iterator[String] = {
    files.toIterator.flatMap { file =>
      if (!filehand.is_directory(file))
        Iterator(file)
      else
        iter_files_recursively(filehand, filehand.list_files(file))
    }
  }

  /**
   * Add "Processing ..." messages when processing each file, and when
   * processing the first file of a directory.
   */
  def iter_files_with_message(filehand: FileHandler,
    files: Iterator[String]) = {
    var lastdir: String = null
    for (file <- files) yield {
      var (dir, fname) = filehand.split_filename(file)
      if (dir != lastdir) {
        errprint("Processing directory %s..." format dir)
        lastdir = dir
      }
      errprint("Processing file %s..." format file)
      file
    }
  }

  /**
   * Iterate over the given files, recursively processing the files in
   * each directory given and displaying a message as each file is
   * processed.
   */
  def iter_files_recursively_with_message(filehand: FileHandler,
      files: Iterable[String]) = {
    iter_files_with_message(filehand,
      iter_files_recursively(filehand, files))
  }

  ////////////////////////////////////////////////////////////////////////////
  //                             File Splitting                             //
  ////////////////////////////////////////////////////////////////////////////

  // Return the next file to output to, when the instances being output to the
  // files are meant to be split according to SPLIT_FRACTIONS.  The absolute
  // quantities in SPLIT_FRACTIONS don't matter, only the values relative to
  // the other values, i.e. [20, 60, 10] is the same as [4, 12, 2].  This
  // function implements an algorithm that is deterministic (same results
  // each time it is run), and spreads out the instances as much as possible.
  // For example, if all values are equal, it will cycle successively through
  // the different split files; if the values are [1, 1.5, 1], the output
  // will be [1, 2, 3, 2, 1, 2, 3, ...]; etc.
  
  def next_split_set(split_fractions: Seq[Double]): Iterable[Int] = {
  
    val num_splits = split_fractions.length
    val cumulative_articles = mutable.Seq.fill(num_splits)(0.0)
  
    // Normalize so that the smallest value is 1.
  
    val minval = split_fractions min
    val normalized_split_fractions =
      (for (value <- split_fractions) yield value.toDouble/minval)
  
    // The algorithm used is as follows.  We cycle through the output sets in
    // order; each time we return a set, we increment the corresponding
    // cumulative count, but before returning a set, we check to see if the
    // count has reached the corresponding fraction and skip this set if so.
    // If we have run through an entire cycle without returning any sets,
    // then for each set we subtract the fraction value from the cumulative
    // value.  This way, if the fraction value is not a whole number, then
    // any fractional quantity (e.g. 0.6 for a value of 7.6) is left over,
    // any will ensure that the total ratios still work out appropriately.
 
    def fuckme_no_yield(): Stream[Int] = {
      var yieldme = mutable.Buffer[Int]()
      for (j <- 0 until num_splits) {
        //println("j=%s, this_output=%s" format (j, this_output))
        if (cumulative_articles(j) < normalized_split_fractions(j)) {
          yieldme += j
          cumulative_articles(j) += 1
        }
      }
      if (yieldme.length == 0) {
        for (j <- 0 until num_splits) {
          while (cumulative_articles(j) >= normalized_split_fractions(j))
            cumulative_articles(j) -= normalized_split_fractions(j)
        }
      }
      yieldme.toStream ++ fuckme_no_yield()
    }
    fuckme_no_yield()
  }

  ////////////////////////////////////////////////////////////////////////////
  //                               Subprocesses                             //
  ////////////////////////////////////////////////////////////////////////////

  /**
   * Run a subprocess and capture its output.  Arguments given are those
   * that will be passed to the subprocess.
   */

  def capture_subprocess_output(args: String*) = {
    val output = new StringBuilder()
    val proc = new ProcessBuilder(args: _*).start()
    val in = proc.getInputStream()
    val br = new BufferedReader(new InputStreamReader(in))
    val cbuf = new Array[Char](100)
    var numread = 0
    /* SCALABUG: The following compiles but will give incorrect results because
       the result of an assignment is Unit! (You do get a warning but ...)
     
     while ((numread = br.read(cbuf, 0, cbuf.length)) != -1)
       output.appendAll(cbuf, 0, numread)

     */
    numread = br.read(cbuf, 0, cbuf.length)
    while (numread != -1) {
      output.appendAll(cbuf, 0, numread)
      numread = br.read(cbuf, 0, cbuf.length)
    }
    proc.waitFor()
    in.close()
    output.toString
  }
}
