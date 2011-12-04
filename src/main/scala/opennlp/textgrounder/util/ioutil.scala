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
import java.util.NoSuchElementException

import org.apache.commons.compress.compressors.bzip2._
import org.apache.commons.compress.compressors.gzip._

import printutil.{errprint, warning}
import textutil._
import osutil._

/**
 * A 'package object' declaration creates a new subpackage and puts the
 * stuff here directly in package scope.  This makes it possible to have
 * functions in package scope instead of inside a class or object (i.e.
 * singleton class).  The functions here are accessed using
 * 'import opennlp.textgrounder.util.ioutil._' outside of package 'util',
 * and simply 'import ioutil._' inside of it.  Note that this is named
 * 'ioutil' instead of just 'io' to avoid possible conflicts with 'scala.io',
 * which is visible by default as 'io'. (Merely declaring it doesn't cause
 * a problem, as it overrides 'scala.io'; but people using 'io.*' either
 * elsewhere in this package or anywhere that does an import of
 * 'opennlp.textgrounder.util._', expecting it to refer to 'scala.io', will
 * be surprised.
 */

package object ioutil {

  //////////////////////////////////////////////////////////////////////////////
  //                            File reading functions                        //
  //////////////////////////////////////////////////////////////////////////////
  
  case class FileFormatException(
    message: String
  ) extends Exception(message) { }

  /**
   * Iterator that yields lines in a given encoding (by default, UTF-8) from
   * an input stream, usually with any terminating newline removed and usually
   * with automatic closing of the stream when EOF is reached.
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
    protected def getNextLine() = {
      nextline = reader.readLine()
      if (nextline == null) {
        if (close)
          reader.close()
        reader = null
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
    def get_output_stream(filename: String, bufsize: Int = 0) = {
      val raw_out = get_raw_output_stream(filename)
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
     *   of the file (minus any extension like .gz or .bzip2).
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
        compression: String = "none", bufsize: Int = 0) = {
      val realname = compression match {
        case "gzip" => GzipUtils.getCompressedFilename(filename)
        case "bzip2" => BZip2Utils.getCompressedFilename(filename)
        case "none" => filename
        case _ => throw new IllegalArgumentException(
          "Invalid compression argument: %s" format compression)
        }
      val raw_out = get_output_stream(realname, bufsize)
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
    def openw(filename: String, encoding: String = "UTF-8",
        compression: String = "none", bufsize: Int = 0,
        autoflush: Boolean = false) = {
      val (out, _) =
        get_output_stream_handling_compression(filename, compression,
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
    def get_raw_output_stream(filename: String): OutputStream
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
    def get_raw_input_stream(filename: String) = new FileInputStream(filename)
    def get_raw_output_stream(filename: String) = new FileOutputStream(filename)
    def split_filename(filename: String) = {
      val file = new File(filename)
      (file.getParent, file.getName)
    }
    def join_filename(dir: String, file: String) =
      new File(dir, file).toString
    def is_directory(filename: String) =
      new File(filename).isDirectory
    def make_directories(filename: String):Boolean =
      new File(filename).mkdirs
    def list_files(dir: String) =
      for (file <- new File(dir).listFiles)
        yield file.toString
  }

  val local_file_handler = new LocalFileHandler

  /* NOTE: Following is the original Python code, which worked slightly
     differently and had a few additional features:

     -- You could pass in a list of files and it would iterate through
        all files in turn; you could pass in no files, in which case it
        would read from stdin.
     -- You could specify the way of handling errors when doing Unicode
        encoding. (FIXME: How do we do this in Java?)
     -- You could also specify a read mode.  This was primarily useful
        for controlling the way that line endings are handled -- e.g.
        "rU" or "U" turns on "universal newline" support, where the
        various kinds of newline endings are automatically converted to
        '\n'; and "rb", which turns on "binary" mode, which forces
        newline conversion *not* to happen even on systems where it is
        the default (particularly, on Windows, where text files are
        terminated by '\r\n', which is normally converted to '\n' on
        input).  Currently, when 'chomp' is true, we automatically
        chomp off all kinds of newlines (whether '\n', '\r' or '\r\n');
        otherwise, we do what the system wants to do by default.
     -- You could specify "in-place modification".  This is built into
        the underlying 'fileinput' module in Python and works like the
        similar feature in Perl.  If you turn the feature on, the input
        file (which cannot be stdin) is renamed upon input, and stdout
        is opened so it writes to a file with the original name.
        The backup file is normally formed by appending '.bak', and
        is deleted automatically on close; but if the 'backup' argument
        is given, the backup file will be maintained, and will be named
        by appending the string given as the value of the argument.
    */
    
  
  ///// 1. chompopen():
  /////
  ///// A generator that yields lines from a file, with any terminating newline
  ///// removed (but no other whitespace removed).  Ensures that the file
  ///// will be automatically closed under all circumstances.
  /////
  ///// 2. openr():
  /////
  ///// Same as chompopen() but specifically open the file as 'utf-8' and
  ///// return Unicode strings.
  
  //"""
  //Test gopen
  //
  //import nlputil
  //for line in nlputil.gopen("foo.txt"):
  //  print line
  //for line in nlputil.gopen("foo.txt", chomp=true):
  //  print line
  //for line in nlputil.gopen("foo.txt", encoding="utf-8"):
  //  print line
  //for line in nlputil.gopen("foo.txt", encoding="utf-8", chomp=true):
  //  print line
  //for line in nlputil.gopen("foo.txt", encoding="iso-8859-1"):
  //  print line
  //for line in nlputil.gopen(["foo.txt"], encoding="iso-8859-1"):
  //  print line
  //for line in nlputil.gopen(["foo.txt"], encoding="utf-8"):
  //  print line
  //for line in nlputil.gopen(["foo.txt"], encoding="iso-8859-1", chomp=true):
  //  print line
  //for line in nlputil.gopen(["foo.txt", "foo2.txt"], encoding="iso-8859-1", chomp=true):
  //  print line
  //"""

//  // General function for opening a file, with automatic closure after iterating
//  // through the lines.  The encoding can be specified (e.g. "utf-8"), and if so,
//  // the error-handling can be given.  Whether to remove the final newline
//  // (chomp=true) can be specified.  The filename can be either a regular
//  // filename (opened with open) or codecs.open(), or a list of filenames or
//  // None, in which case the argument is passed to fileinput.input()
//  // (if a non-empty list is given, opens the list of filenames one after the
//  // other; if an empty list is given, opens stdin; if None is given, takes
//  // list from the command-line arguments and proceeds as above).  When using
//  // fileinput.input(), the arguments "inplace", "backup" and "bufsize" can be
//  // given, appropriate to that function (e.g. to do in-place filtering of a
//  // file).  In all cases, 
//  def gopen(filename, mode="r", encoding=None, errors="strict", chomp=false,
//      inplace=0, backup="", bufsize=0):
//    if isinstance(filename, basestring):
//      def yieldlines():
//        if encoding is None:
//          mgr = open(filename)
//        else:
//          mgr = codecs.open(filename, mode, encoding=encoding, errors=errors)
//        with mgr as f:
//          for line in f:
//            yield line
//      iterator = yieldlines()
//    else:
//      if encoding is None:
//        openhook = None
//      else:
//        def openhook(filename, mode):
//          return codecs.open(filename, mode, encoding=encoding, errors=errors)
//      iterator = fileinput.input(filename, inplace=inplace, backup=backup,
//          bufsize=bufsize, mode=mode, openhook=openhook)
//    if chomp:
//      for line in iterator:
//        if line and line[-1] == "\n": line = line[:-1]
//        yield line
//    else:
//      for line in iterator:
//        yield line
//  
//  // Open a filename and yield lines, but with any terminating newline
//  // removed (similar to "chomp" in Perl).  Basically same as gopen() but
//  // with defaults set differently.
//  def chompopen(filename, mode="r", encoding=None, errors="strict",
//      chomp=true, inplace=0, backup="", bufsize=0):
//    return gopen(filename, mode=mode, encoding=encoding, errors=errors,
//        chomp=chomp, inplace=inplace, backup=backup, bufsize=bufsize)
//  
//  // Open a filename with UTF-8-encoded input.  Basically same as gopen()
//  // but with defaults set differently.
//  def uopen(filename, mode="r", encoding="utf-8", errors="strict",
//      chomp=false, inplace=0, backup="", bufsize=0):
//    return gopen(filename, mode=mode, encoding=encoding, errors=errors,
//        chomp=chomp, inplace=inplace, backup=backup, bufsize=bufsize)
//

  /**
   * Class that lets you process a series of files in turn; if any file
   * names a directory, all files in the directory will be processed.
   * If a file is given as 'null', that will be passed on unchanged.
   * (Useful to signal input taken from an internal source.)
   */
  abstract class FileProcessor {

    /**
     * Process all files, calling `process_file` on each.
     *
     * @param files Files to process.  If any file names a directory,
     *   all files in the directory will be processed.  If any file
     *   is null, it will be passed on unchanged (see above; useful
     *   e.g. for specifying input from an internal source).
     * @param output_messages If true, output messages indicating the
     *   files being processed.
     * @return True if file processing continued to completion,
     *   false if interrupted because an invocation of `process_file`
     *   returns false.
     */
    def process_files(filehand: FileHandler, files: Iterable[String],
        output_messages: Boolean = true) = {
      var broken = false
      begin_processing(filehand, files)
      breakable {
        def process_one_file(filename: String) {
          if (output_messages && filename != null)
            errprint("Processing file %s..." format filename)
          begin_process_file(filehand, filename)
          if (!process_file(filehand, filename)) {
            // This works because of the way 'breakable' is implemented
            // (dynamically-scoped).  Might "break" (stop working) if break
            // is made totally lexically-scoped.
            broken = true
          }
          end_process_file(filehand, filename)
          if (broken)
            break
        }
        for (dir <- files) {
          if (dir == null)
            process_one_file(dir)
          else {
            if (filehand.is_directory(dir)) {
              if (output_messages)
                errprint("Processing directory %s..." format dir)
              begin_process_directory(filehand, dir)
              val files = filehand.list_files(dir)
              for (file <- filter_dir_files(filehand, dir, files)) {
                process_one_file(file)
              }
              end_process_directory(filehand, dir)
            } else process_one_file(dir)
          }
        }
      }
      end_processing(filehand, files)
      !broken
    }

    /*********************** MUST BE IMPLEMENTED *************************/

    /**
     * Process a given file.
     *
     * @param filehand The FileHandler for working with the file.
     * @param file The file to process (possibly null, see above).
     * @return True if file processing should continue; false to
     *   abort any further processing.
     */
    def process_file(filehand: FileHandler, file: String): Boolean

    /***************** MAY BE IMPLEMENTED (THROUGH OVERRIDING) ***************/

    /**
     * Called when about to begin processing all files in a directory.
     * Must be overridden, since it has an (empty) definition by default.
     *
     * @param filehand The FileHandler for working with the file.
     * @param dir Directory being processed.
     */
    def begin_process_directory(filehand: FileHandler, dir: String) {
    }

    /**
     * Called when finished processing all files in a directory.
     * Must be overridden, since it has an (empty) definition by default.
     *
     * @param filehand The FileHandler for working with the file.
     * @param dir Directory being processed.
     */
    def end_process_directory(filehand: FileHandler, dir: String) {
    }

    /**
     * Called when about to begin processing a file.
     * Must be overridden, since it has an (empty) definition by default.
     *
     * @param filehand The FileHandler for working with the file.
     * @param file File being processed.
     */
    def begin_process_file(filehand: FileHandler, file: String) {
    }

    /**
     * Called when finished processing a file.
     * Must be overridden, since it has an (empty) definition by default.
     *
     * @param filehand The FileHandler for working with the file.
     * @param file File being processed.
     */
    def end_process_file(filehand: FileHandler, file: String) {
    }

    /**
     * Called when about to begin processing files.
     * Must be overridden, since it has an (empty) definition by default.
     *
     * @param filehand The FileHandler for working with the file.
     * @param dir Directory being processed.
     */
    def begin_processing(filehand: FileHandler, files: Iterable[String]) {
    }

    /**
     * Called when finished processing all files.
     * Must be overridden, since it has an (empty) definition by default.
     *
     * @param filehand The FileHandler for working with the file.
     * @param dir Directory being processed.
     */
    def end_processing(filehand: FileHandler, files: Iterable[String]) {
    }

    /**
     * Called when opening a directory to filter out unwanted files.
     * Takes a list of files, returns the filtered list of files.
     * Must be overridden, since its default definition is simply to
     * return all files in the directory.
     *
     * @param filehand The FileHandler for working with the files.
     * @param dir Directory being processed.
     * @param files The list of files in the directory.
     *
     * @return The filtered list.
     */
    def filter_dir_files(filehand: FileHandler, dir: String,
        files: Iterable[String]) = files
  }

  /**
   * Class that lets you process a series of text files in turn, using
   * the same mechanism for processing the files themselves as in
   * `FileProcessor`.
   */
  abstract class TextFileProcessor extends FileProcessor {
    /**
     * Process a given file.
     *
     * @param filehand The FileHandler for working with the file.
     * @param file The file to process (possibly null, see above).
     * @return True if file processing should continue; false to
     *   abort any further processing.
     */
    def process_file(filehand: FileHandler, file: String) = {
      val (lines, compression, realname) =
        filehand.openr_with_compression_info(file)
      begin_process_lines(lines, filehand, file, compression, realname)
      process_lines(lines, filehand, file, compression, realname)
    }

    /*********************** MUST BE IMPLEMENTED *************************/

    /**
     * Called to process the lines of a file.
     * Must be overridden, since it has an (empty) definition by default.
     *
     * @param lines Iterator over the lines in the file.
     * @param filehand The FileHandler for working with the file.
     * @param file The name of the file being processed.
     * @param compression The compression of the file ("none" for no
     *   compression).
     * @param realname The "real" name of the file, after any compression
     *   suffix (e.g. .gz, .bzip2) is removed.
     */
    def process_lines(lines: Iterator[String],
        filehand: FileHandler, file: String,
        compression: String, realname: String): Boolean

    /***************** MAY BE IMPLEMENTED (THROUGH OVERRIDING) ***************/

    /**
     * Called when about to begin processing the lines from a file.
     * Must be overridden, since it has an (empty) definition by default.
     * Note that this is generally called just once per file, just like
     * `begin_process_file`; but this function has compression info and
     * the line iterator available to it.
     *
     * @param filehand The FileHandler for working with the file.
     * @param file File being processed.
     */
    def begin_process_lines(lines: Iterator[String],
        filehand: FileHandler, file: String,
        compression: String, realname: String) { }

    /*
       The following may be implemented, but have default, mostly empty,
       definitions.  These are the same as for the base FileProcessor.

    def begin_process_directory(filehand: FileHandler, dir: String) {
    }
    def end_process_directory(filehand: FileHandler, dir: String) {
    }
    def begin_processing(filehand: FileHandler, files: Iterable[String]) {
    }
    def end_processing(filehand: FileHandler, files: Iterable[String]) {
    }
    def filter_dir_files(filehand: FileHandler, dir: String,
        files:Seq[String]) = files
    */
  }

  /**
   * A text-file processor where each line is made up of a fixed number
   * of fields, separated by some sort of separator (by default a tab
   * character).  No implementation is provided for `process_lines`,
   * the driver function.  This function in general should loop over the
   * lines, calling `parse_row` on each one.
   *
   * @param split_re Regular expression used to split one field from another.
   *   By default a tab character.
   */

  abstract class FieldTextFileProcessor(
    split_re: String = "\t"
  ) extends TextFileProcessor {
    var all_num_processed = 0
    var all_num_bad = 0
    var num_processed = 0
    var num_bad = 0

    var fieldnames: Seq[String] = _

    /**
     * Set the field names used for processing rows.
     */
    def set_fieldnames(fieldnames: Seq[String]) {
      this.fieldnames = fieldnames
    }

    override def begin_process_file(filehand: FileHandler, file: String) {
      num_processed = 0
      num_bad = 0
      super.begin_process_file(filehand, file)
    }

    override def end_process_file(filehand: FileHandler, file: String) {
      all_num_processed += num_processed
      all_num_bad += num_bad
      super.end_process_file(filehand, file)
    }

    /**
     * Parse a given row into fields.  Call either #process_row or
     * #handle_bad_row.
     */
    def parse_row(line: String) {
      // println("[%s]" format line)
      val fieldvals = line.split(split_re, -1)
      if (fieldvals.length != fieldnames.length)
        handle_bad_row(line, fieldvals)
      else {
        val good = process_row(fieldvals)
        if (!good)
          handle_bad_row(line, fieldvals)
      }
      num_processed += 1
    }

    /*********************** MUST BE IMPLEMENTED *************************/

    /**
     * Called when a "good" row is seen (good solely in that it has the
     * correct number of fields).
     *
     * @param fieldvals: List of the string values for each field.
     *
     * @return True if the line is truly "good"; false, otherwise. (In the
     *   latter case, `handle_bad_row` will be called.)
     */
    def process_row(fieldvals: Seq[String]): Boolean

    /* Also,

    def process_lines(lines: Iterator[String],
        filehand: FileHandler, file: String,
        compression: String, realname: String): Boolean

       A simple implementation simply loops over all the lines and calls
       parse_row() on each one.
    */

    /******************* MAY BE IMPLEMENTED (OVERRIDDEN) *******************/

    /**
     * Called when a bad row is seen.  By default, output a warning.
     *
     * @param line Text of the row.
     * @param fieldvals Field values parsed from the row.
     */
    def handle_bad_row(line: String, fieldvals: Seq[String]) {
      val lineno = num_processed + 1
      if (fieldnames.length != fieldvals.length) {
        warning(
          """Line %s: Bad record, expected %s fields, saw %s fields;
          skipping line=%s""", lineno, fieldnames.length, fieldvals.length,
          line)
      } else {
        warning("""Line %s: Bad record; skipping line=%s""", lineno, line)
      }
      num_bad += 1
    }

    /* Also, the same "may-be-implemented" functions from the superclass
       TextFileProcessor. */
  }

  class FieldTextWriter(
    schema: Schema,
    split_text: String = "\t"
  ) {
    def output_schema(outstream: PrintStream) {
      outstream.println(schema.fieldnames mkString split_text)
      for ((field, value) <- schema.fixed_values)
        outstream.println(List(field, value) mkString split_text)
    }

    def output_row(outstream: PrintStream, fieldvals: Iterable[String]) {
      val seqvals = fieldvals.toSeq
      assert(seqvals.length == schema.fieldnames.length)
      outstream.println(seqvals mkString split_text)
    }
  }

  case class Schema(
    fieldnames: Seq[String],
    fixed_values: mutable.LinkedHashMap[String, String]
  ) {
    def get_field(fieldvals: Seq[String], key: String,
        error_if_missing: Boolean = true) =
      get_field_or_else(fieldvals, key, error_if_missing = error_if_missing)

    def get_field_or_else(fieldvals: Seq[String], key: String,
        default: String = null, error_if_missing: Boolean = false): String = {
      assert(fieldvals.length == fieldnames.length)
      var i = 0
      while (i < fieldnames.length) {
        if (fieldnames(i) == key) return fieldvals(i)
        i += 1
      }
      return get_fixed_field(key, default, error_if_missing)
    }

    def get_fixed_field(key: String, default: String = null,
        error_if_missing: Boolean = false) = {
      if (fixed_values contains key)
        fixed_values(key)
      else if (error_if_missing) {
        throw new NoSuchElementException("key not found: %s" format key)
      } else default
    }
  }

  object Schema {
    /**
     * Read the given schema file.
     *
     * @param filehand File handler of schema file name.
     * @param schema_file Name of the schema file.
     * @param split_re Regular expression used to split the fields of the
     *   schema file, usually TAB. (There's only one row, and each field in
     *   the row gives the name of the corresponding field in the document
     *   file.)
     */
    def read_schema_file(filehand: FileHandler, schema_file: String,
        split_re: String = "\t") = {
      val lines = filehand.openr(schema_file)
      val fieldname_line = lines.next()
      val fieldnames = fieldname_line.split(split_re, -1)
      for (field <- fieldnames if field.length == 0)
        throw new FileFormatException(
          "Blank field name in schema file %s: fields are %s".
          format(schema_file, fieldnames))
      val fixed_fields = mutable.LinkedHashMap[String,String]()
      for (line <- lines) {
        val fixed = line.split(split_re, -1)
        if (fixed.length != 2)
          throw new FileFormatException(
            "For fixed fields (i.e. lines other than first) in schema file %s, should have two values (FIELD and VALUE), instead of %s".
            format(schema_file, line))
        val Array(from, to) = fixed
        if (from.length == 0)
          throw new FileFormatException(
            "Blank field name in fxed-value part of schema file %s: line is %s".
              format(schema_file, line))
        fixed_fields(from) = to
      }
      new Schema(fieldnames, fixed_fields)
    }
  }

  /**
   * File processor for reading in a "corpus" of documents.  The corpus
   * has the following format:
   *
   * (1) The documents are stored as field-text files, separated by a TAB
   *     character.
   * (2) There is a corresponding schema file, which lists the names of
   *     each field, separated by a TAB character, as well as any
   *     "fixed" fields that have the same value for all rows (one per
   *     line, with the name, a TAB, and the value).
   * (3) The document and schema files are identified by a SUFFIX.
   *     The document files are named `*-SUFFIX.txt` (or `*-SUFFIX.txt.bz2` or
   *     similar), while the schema file is named `*-SUFFIX-schema.txt`.
   *
   * It's generally assumed that the document and schema files are placed
   * in a single directory, and that only one corpus with a given suffix
   * is placed in that directory (although corpora with different suffixes
   * can coexist in the same directory).  This makes it possible to locate
   * the schema and document files by looking for appropriately-named
   * files in a given directory, while the files themselves can have
   * arbitrary descriptive prefixes identifying the particular corpus
   * involved.
   *
   * Generally, after creating a file processor of this sort, the schema
   * file needs to be read using `read_schema_from_corpus`; then the document
   * files can be processed using `process_files` (which typically takes the
   * directory name as its argument).
   *
   * @param suffix the suffix of the corpus files, as described above
   *     
   */
  abstract class CorpusFileProcessor(
    suffix: String
  ) extends FieldTextFileProcessor {
    import CorpusFileProcessor._

    var schema_file: String = _
    var schema_file_filehand: FileHandler = _
    var schema: Schema = _

    def set_schema(schema: Schema) {
      this.schema = schema
      set_fieldnames(schema.fieldnames)
    }

    /**
     * Locate the schema file of the appropriate suffix in the given directory.
     */
    def find_schema_file(filehand: FileHandler, dir: String) = {
      val schema_regex = make_schema_file_suffix_regex(suffix)
      val all_files = filehand.list_files(dir)
      val files =
        (for (file <- all_files
          if schema_regex.findFirstMatchIn(file) != None) yield file).toSeq
      if (files.length == 0)
        throw new FileFormatException(
          "Found no schema files (matching %s) in directory %s"
          format (schema_regex, dir))
      if (files.length > 1)
        throw new FileFormatException(
          "Found multiple schema files (matching %s) in directory %s: %s"
          format (schema_regex, dir, files))
      files(0)
    }

    /**
     * Locate and read the schema file of the appropriate suffix in the
     * given directory.  Set internal variables containing the schema file
     * and schema.
     */
    def read_schema_from_corpus(filehand: FileHandler, dir: String) {
      schema_file = find_schema_file(filehand, dir)
      schema_file_filehand = filehand
      val schema = Schema.read_schema_file(filehand, schema_file)
      set_schema(schema)
    }

    /**
     * Filter function to restrict the files processed to only the
     * document files of the appropriate suffix.
     */
    override def filter_dir_files(filehand: FileHandler, dir: String,
        files: Iterable[String]) = {
      val filter = make_document_file_suffix_regex(suffix)
      for (file <- files if filter.findFirstMatchIn(file) != None) yield file
    }
  }

  object CorpusFileProcessor {
    val possible_compression_re = """(\.[a-zA-Z0-9]+)?$"""
    /**
     * For a given suffix, create a regular expression
     * ([[scala.util.matching.Regex]]) that matches document files of the
     * suffix.
     */
    def make_document_file_suffix_regex(suffix: String) = {
      val re_quoted_suffix = """-%s\.txt""" format suffix
      (re_quoted_suffix + possible_compression_re).r
    }
    /**
     * For a given suffix, create a regular expression
     * ([[scala.util.matching.Regex]]) that matches schema files of the
     * suffix.
     */
    def make_schema_file_suffix_regex(suffix: String) = {
      val re_quoted_suffix = """-%s-schema\.txt""" format suffix
      (re_quoted_suffix + possible_compression_re).r
    }

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

  // The original Python implementation, which had more functionality:

  /*
    Run the specified command; return its output (usually, the combined
    stdout and stderr output) as a string.  'command' can either be a
    string or a list of individual arguments.  Optional argument 'shell'
    indicates whether to pass the command to the shell to run.  If
    unspecified, it defaults to true if 'command' is a string, false if
    a list.  If optional arg 'input' is given, pass this string as the
    stdin to the command.  If 'include_stderr' is true (the default),
    stderr will be included along with the output.  If return code is
    non-zero, throw CommandError if 'throw' is specified; else, return
    tuple of (output, return-code).
  */

//  def backquote(command, input=None, shell=None, include_stderr=true, throw=true):
//    //logdebug("backquote called: %s" % command)
//    if shell is None:
//      if isinstance(command, basestring):
//        shell = true
//      else:
//        shell = false
//    stderrval = STDOUT if include_stderr else PIPE
//    if input is not None:
//      popen = Popen(command, stdin=PIPE, stdout=PIPE, stderr=stderrval,
//                    shell=shell, close_fds=true)
//      output = popen.communicate(input)
//    else:
//      popen = Popen(command, stdout=PIPE, stderr=stderrval,
//                    shell=shell, close_fds=true)
//      output = popen.communicate()
//    if popen.returncode != 0:
//      if throw:
//        if output[0]:
//          outputstr = "Command's output:\n%s" % output[0]
//          if outputstr[-1] != '\n':
//            outputstr += '\n'
//        errstr = output[1]
//        if errstr and errstr[-1] != '\n':
//          errstr += '\n'
//        errmess = ("Error running command: %s\n\n%s\n%s" %
//            (command, output[0], output[1]))
//        //log.error(errmess)
//        oserror(errmess, EINVAL)
//      else:
//        return (output[0], popen.returncode)
//    return output[0]
//  
//  def oserror(mess, err):
//    e = OSError(mess)
//    e.errno = err
//    raise e

}
