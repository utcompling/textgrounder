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

package opennlp.textgrounder.geolocate

import util.control.Breaks._
import collection.mutable
import collection.mutable.{Builder, MapBuilder}
import collection.generic.CanBuildFrom
import math._
// The following says to import everything except java.io.Console, because
// it conflicts with (and overrides) built-in scala.Console. (Technically,
// it imports everything but in the process aliases Console to _, which
// has the effect of making it inaccessible. _ is special in Scala and has
// various meanings.)
import java.io.{Console=>_,_}
import java.util.Date
import java.text.DateFormat

// import bisect // For sorted lists
// from heapq import * // For priority queue
// import resource // For resource usage
// from collections import deque // For breadth-first search

/**
 * A 'package object' declaration creates a new subpackage and puts the
 * stuff here directly in package scope.  This makes it possible to have
 * functions in package scope instead of inside a class or object (i.e.
 * singleton class).  This package is called 'tgutil' rather than simply
 * 'util' to avoid conflicting with Scala's 'util' package.  The functions
 * here are accessed using 'import tgutil._' inside of the package
 * opennlp.textgrounder.geolocate, and using
 * 'import opennlp.textgrounder.geolocate.tgutil._' elsewhere.
 */

package object tgutil {

  //////////////////////////////////////////////////////////////////////////////
  //                            File reading functions                        //
  //////////////////////////////////////////////////////////////////////////////
  
  // Open a filename with the given encoding (by default, UTF-8) and
  // yield lines, but with any terminating newline removed if chomp is
  // true (the default).  Buffer size and conversion error-handling
  // can be set (FIXME: The latter has no effect currently.)
  def openr(filename: String, encoding: String= "UTF-8", chomp: Boolean= true,
        errors: String= "strict", bufsize: Int= 0) = {
    class FileIterator extends Iterator[String] {
      val ireader =
        new InputStreamReader(new FileInputStream(filename), encoding)
      var reader =
        if (bufsize <= 0) new BufferedReader(ireader)
        else new BufferedReader(ireader, bufsize)
      var nextline: String = null
      protected def getNextLine() = {
        nextline = reader.readLine()
        if (nextline == null) {
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
        if (!hasNext) null
        else {
          val ret = nextline
          nextline = null
          ret
        }
      }
    }

    new FileIterator
  }
  
  /** Open a file for writing and return a PrintStream that will write to
   *  this file in UTF-8.
   */
  def openw(filename: String, autoflush: Boolean=false) = new PrintStream(
      new BufferedOutputStream(new FileOutputStream(filename)),
      autoflush,
      "UTF-8")

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
     * Process a given file.
     *
     * @param file The file to process (possibly null, see above).
     * @returns True if file processing should continue; false to
     *   abort any further processing.
     */
    def process_file(file: String): Boolean

    /**
     * Called when about to begin processing all files in a directory.
     * Must be overridden, since it has an (empty) definition by default.
     *
     * @param dir File object for the directory.
     */
    def begin_process_directory(dir: File) {
    }

    /**
     * Process all files, calling `process_file` on each.
     *
     * @param files Files to process.  If any file names a directory,
     *   all files in the directory will be processed.  If any file
     *   is null, it will be passed on unchanged (see above; useful
     *   e.g. for specifying input from an internal source).
     */
    def process_files(files: Iterable[String]) {
      breakable {
        def process_one_file(filename: String) {
          if (!process_file(filename))
            // This works because of the way 'breakable' is implemented
            // (dynamically-scoped).  Might "break" (stop working) if break
            // is made totally lexically-scoped.
            break
        }
        for (dir <- files) {
          if (dir == null)
            process_one_file(dir)
          else {
            val dirfile = new File(dir)
            if (dirfile.isDirectory) {
              for (file <- dirfile.listFiles().toSeq) {
                val filename = file.toString
                process_one_file(filename)
              }
            } else process_one_file(dir)
          }
        }
      }
    }
  }

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

  def uniprint(text: String, outfile: PrintStream=System.out) {
    outfile.println(text)
  }
  def uniout(text: String, outfile: PrintStream=System.out) {
    outfile.print(text)
  }
  
  def errprint(format: String, args: Any*) {
    // If no arguments, assume that we've been passed a raw string to print,
    // so print it directly rather than passing it to 'format', which might
    // munge % signs
    if (args.length == 0)
      System.err.println(format)
    else
      System.err.println(format format (args: _*))
  }
  def errout(format: String, args: Any*) {
    if (args.length == 0)
      System.err.print(format)
    else
      System.err.print(format format (args: _*))
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
  //                    String functions involving numbers                  //
  ////////////////////////////////////////////////////////////////////////////

  /**
   Convert a string to floating point, but don't crash on errors;
  instead, output a warning.
   */
  def safe_float(x: String) = {
    try {
      x.toDouble
    } catch {
      case _ => {
        val y = x.trim()
        if (y != "") warning("Expected number, saw %s", y)
        0.
      }
    }
  }

  // Originally based on code from:
  // http://stackoverflow.com/questions/1823058/how-to-print-number-with-commas-as-thousands-separators-in-python-2-x
  def long_with_commas(x: Long): String = {
    var mx = x
    if (mx < 0)
      return "-" + long_with_commas(-mx)
    var result = ""
    while (mx >= 1000) {
      val r = mx % 1000
      mx /= 1000
      result = ",%03d%s" format (r, result)
    }
    return "%d%s" format (mx, result)
  }
  
  // My own version
  def float_with_commas(x: Double) = {
    val intpart = floor(x).toInt
    val fracpart = x - intpart
    long_with_commas(intpart) + ("%.2f" format fracpart).drop(1)
  }
 
  // Try to format something with reasonable precision.
  def format_float(x: Double) = {
    var precision = 2
    var xx = x
    while (xx < 0.1) {
      xx *= 10
      precision += 1
    }
    val formatstr = "%%.%sf" format precision
    formatstr format x
  }

  def format_minutes_seconds(seconds: Double) = {
    var secs = seconds
    var mins = (secs / 60).toInt
    secs = secs % 60
    val hours = (mins / 60).toInt
    mins = mins % 60
    var hourstr = (
      if (hours > 0) "%s hour%s " format (hours, if (hours == 1) "" else "s")
      else "")
    val secstr = (if (secs.toInt == secs) "%s" else "%1.1f") format secs
    "%s%s minute%s %s second%s" format (
        hourstr,
        mins, if (mins == 1) "" else "s",
        secstr, if (secs == 1) "" else "s")
  }
  
  ////////////////////////////////////////////////////////////////////////////
  //                           Other string functions                       //
  ////////////////////////////////////////////////////////////////////////////

  // A function to make up for a missing feature in Scala.  Split a text
  // into segments but also return the delimiters.  Regex matches the
  // delimiters.  Return a list of tuples (TEXT, DELIM).  The last tuple
  // with have an empty delim.
  def re_split_with_delimiter(regex: util.matching.Regex, text: String) = {
    val splits = regex.split(text)
    val delim_intervals =
      for (m <- regex.findAllIn(text).matchData) yield List(m.start, m.end)
    val flattened = List(0) ++ (delim_intervals reduce (_ ++ _)) ++
      List(text.length, text.length)
    val interval_texts = flattened.iterator.sliding(2) map (
        x => {
          val Seq(y,z) = x
          text.slice(y,z)
        }
      )
    interval_texts grouped 2
  }

  /* A function to make up for a bug in Scala.  The normal split() is broken
     in that if the delimiter occurs at the end of the line, it gets ignored;
     in fact, multiple such delimiters at end of line get ignored.  We hack
     around that by adding an extra char at the end and then removing it
     later. */
  def splittext(str: String, ch: Char) = {
    val ch2 = if (ch == 'x') 'y' else 'x'
    val stradd = str + ch2
    val ret = stradd.split(ch)
    ret(ret.length - 1) = ret(ret.length - 1).dropRight(1)
    ret
  }

  // A worse implementation -- it will fail if there are any NUL bytes
  // in the input.
  // def propersplit(str: String, ch: Char) = {
  //   val chs = ch.toString
  //   for (x <- str.replace(chs, chs + "\000").split(ch))
  //     yield x.replace("\000", "")
  // }

  def split_text_into_words(text: String, ignore_punc: Boolean=false,
    include_nl: Boolean=false) = {
    // This regexp splits on whitespace, but also handles the following cases:
    // 1. Any of , ; . etc. at the end of a word
    // 2. Parens or quotes in words like (foo) or "bar"
    // These punctuation characters are returned as separate words, unless
    // 'ignore_punc' is given.  Also, if 'include_nl' is given, newlines are
    // returned as their own words; otherwise, they are treated like all other
    // whitespace (i.e. ignored).
    (for (Seq(word, punc) <-
          re_split_with_delimiter("""([,;."):]*(?:\s+|$)[("]*)""".r, text)) yield
       Seq(word) ++ (
         for (p <- punc; if !(" \t\r\f\013" contains p)) yield (
           if (p == '\n') (if (include_nl) p.toString else "")
           else (if (!ignore_punc) p.toString else "")
         )
       )
    ) reduce (_ ++ _) filter (_ != "")
  }
 
  
  /**
   Pluralize an English word, using a basic but effective algorithm.
   */
  def pluralize(word: String) = {
    val upper = word.last >= 'A' && word.last <= 'Z'
    val lowerword = word.toLowerCase()
    val ies_re = """.*[b-df-hj-np-tv-z]y$""".r
    val es_re = """.*([cs]h|[sx])$""".r
    lowerword match {
      case ies_re() =>
        if (upper) word.dropRight(1) + "IES"
        else word.dropRight(1) + "ies"
      case es_re() =>
        if (upper) word + "ES"
        else word + "es"
      case _ =>
        if (upper) word + "S"
        else word + "s"
    }
  }

  /**
   Capitalize the first letter of string, leaving the remainder alone.
   */
  def capfirst(st: String) = {
    if (st == "") st else st(0).toString.capitalize + st.drop(1)
  }
  
  /*
    A simple object to make regexps a bit less awkward.  Works like this:

    ("foo (.*)", "foo bar") match {
      case Re(x) => println("matched 1 %s" format x)
      case _ => println("no match 1")
    }

    This will print out "matched 1 bar".
   */

  object Re {
    def unapplySeq(x: Tuple2[String, String]) = {
      val (re, str) = x
      re.r.unapplySeq(str)
    }
  }
    
  ////////////////////////////////////////////////////////////////////////////
  //                          Default dictionaries                          //
  ////////////////////////////////////////////////////////////////////////////
  
  abstract class DefaultHashMap[F,T] extends mutable.HashMap[F,T] {
    def getNoSet(key: F): T
  }

  /**
   * Create a default hash table, i.e. a hash table where accesses to
   * undefined values automatically return 'defaultval'.  This class also
   * automatically sets the undefined key to 'defaultval' upon first
   * access to that key.  If you don't want this behavior, call getNoSet()
   * or use the non-setting variant below.  See the discussion below in
   * defaultmap() for a discussion of when setting vs. non-setting is useful
   * (in a nutshell, use the setting variant when type T is a mutable
   * collection; otherwise, use the nonsetting variant).
   */
  class SettingDefaultHashMap[F,T](
    defaultval: => T
  ) extends DefaultHashMap[F,T] {
    var internal_setkey = true

    override def default(key: F) = {
      val buf = defaultval
      if (internal_setkey)
        this(key) = buf
      buf
    }
        
    /**
     * Retrieve the value of 'key'.  If the value isn't found, the
     * default value (from 'defaultval') will be returned, but the
     * key will *NOT* added to the table with that value.
     *
     * FIXME: This code should have the equivalent of
     * synchronized(internal_setkey) around it so that it will work
     * in a multi-threaded environment.
     */
    def getNoSet(key: F) = {
      val oi_setkey = internal_setkey
      try {
        internal_setkey = false
        this(key)
      } finally { internal_setkey = oi_setkey }
    }
  }

  /**
   * Non-setting variant class for creating a default hash table.
   * See class SettingDefaultHashMap and function defaultmap().
   */
  class NonSettingDefaultHashMap[F,T](
    defaultval: => T
  ) extends DefaultHashMap[F,T] {
    override def default(key: F) = {
      val buf = defaultval
      buf
    }
        
    def getNoSet(key: F) = this(key)
  }

  /**
   * Create a default hash map that maps keys of type F to values of type
   * T, automatically returning 'defaultval' rather than throwing an exception
   * if the key is undefined.
   *
   * @param defaultval The default value to return.  Note the delayed
   *   evaluation using =>.  This is done on purpose so that, for example,
   *   if we use mutable Buffers or Sets as the value type, things will
   *   work: We want a *different* empty vector or set each time we call
   *   default(), so that different keys get different empty vectors.
   *   Otherwise, adding an element to the buffer associated with one key
   *   will also add it to the buffers for other keys, which is not what
   *   we want.
   * 
   * @param setkey indicates whether we set the key to the default upon
   *   access.  This is necessary when the value is something mutable, but
   *   probably a bad idea otherwise, since looking up a nonexistent value
   *   in the table will cause a later contains() call to return true on the
   *   value.
   * 
   * For example:
    
    val foo = defaultmap[String,Int](0, setkey = false)
    foo("bar")              -> 0
    foo contains "bar"      -> false
   
    val foo = defaultmap[String,Int](0, setkey = true)
    foo("bar")              -> 0
    foo contains "bar"      -> true         (Probably not what we want)
                   
   
    val foo = defaultmap[String,mutable.Buffer[String]](mutable.Buffer(), setkey = false)
    foo("myfoods") += "spam"
    foo("myfoods") += "eggs"
    foo("myfoods") += "milk"
    foo("myfoods")             -> ArrayBuffer(milk)                (OOOOOPS)
   
    val foo = defaultmap[String,mutable.Buffer[String]](mutable.Buffer(), setkey = true)
    foo("myfoods") += "spam"
    foo("myfoods") += "eggs"
    foo("myfoods") += "milk"
    foo("myfoods")             -> ArrayBuffer(spam, eggs, milk)    (Good)
   */
  def defaultmap[F,T](defaultval: => T, setkey: Boolean = false) = {
    if (setkey) new SettingDefaultHashMap[F,T](defaultval)
    else new NonSettingDefaultHashMap[F,T](defaultval)
  }
  /* These next four are maps from T to Int, Double, Boolean or String,
     which automatically use a default value if the key isn't seen.
     They can be used in some cases where you simply want to be able to
     look anything up, whether set or not; but especially useful when
     accumulating counts and such, where you want to add to the existing
     value and want keys not yet seen to automatically spring into
     existence with the value of 0 (or empty string). */
  def intmap[T]() = defaultmap[T,Int](0)
  def doublemap[T]() = defaultmap[T,Double](0.0)
  def booleanmap[T]() = defaultmap[T,Boolean](false)
  def stringmap[T]() = defaultmap[T,String]("")
  /** A default map which maps from T to an (extendible) array of type U.
      The default value is an empty Buffer of type U.  Calls of the sort
      'map(key) += item' will add the item to the Buffer stored as the
      value of the key rather than changing the value itself. (After doing
      this, the result of 'map(key)' will be the same collection, but the
      contents of the collection will be modified.  On the other hand, in
      the case of the above maps, the result of 'map(key)' will be
      different.)
    */
  def bufmap[T,U]() =
    defaultmap[T,mutable.Buffer[U]](mutable.Buffer[U](), setkey=true)
  
  // Another way to do this, using subclassing.
  //
  // abstract class defaultmap[From,To] extends HashMap[From, To] {
  //   val defaultval: To
  //   override def default(key: From) = defaultval
  // }
  //
  // class intmap[T] extends defaultmap[T, Int] { val defaultval = 0 }
  //

  // The original way
  //
  // def booleanmap[String]() = {
  //   new HashMap[String, Boolean] {
  //     override def default(key: String) = false
  //   }
  // }

  /////////////////////////////////////////////////////////////////////////////
  //                              Dynamic arrays                             //
  /////////////////////////////////////////////////////////////////////////////
  
  /**
   A simple class like ArrayBuilder but which gets you direct access
   to the underlying array and lets you easily reset things, so that you
   can reuse a Dynamic Array multiple times without constantly creating
   new objects.  Also has specialization.
   */
  class DynamicArray[@specialized T:ClassManifest](initial_alloc:Int = 8) {
    protected val multiply_factor = 1.5
    var array = new Array[T](initial_alloc)
    var length = 0
    def ensure_at_least(size: Int) {
      if (array.length < size) {
        var newsize = array.length
        while (newsize < size)
          newsize = (newsize * multiply_factor).toInt
        array = new Array[T](newsize)
      }
    }

    def += (item: T) {
      ensure_at_least(length + 1)
      array(length) = item
      length += 1
    }

    def clear() {
      length = 0
    }
  }
    
  /////////////////////////////////////////////////////////////////////////////
  //                                Sorted lists                             //
  /////////////////////////////////////////////////////////////////////////////
  
  // Return a tuple (keys, values) of lists of items corresponding to a hash
  // table.  Stored in sorted order according to the keys.  Use
  // lookup_sorted_list(key) to find the corresponding value.  The purpose of
  // doing this, rather than just directly using a hash table, is to save
  // memory.

//  def make_sorted_list(table):
//    items = sorted(table.items(), key=lambda x:x[0])
//    keys = [""]*len(items)
//    values = [""]*len(items)
//    for i in xrange(len(items)):
//      item = items[i]
//      keys[i] = item[0]
//      values[i] = item[1]
//    return (keys, values)
//  
//  // Given a sorted list in the tuple form (KEYS, VALUES), look up the item KEY.
//  // If found, return the corresponding value; else return None.
//  
//  def lookup_sorted_list(sorted_list, key, default=None):
//    (keys, values) = sorted_list
//    i = bisect.bisect_left(keys, key)
//    if i != len(keys) and keys[i] == key:
//      return values[i]
//    return default
//  
//  // A class that provides a dictionary-compatible interface to a sorted list
//  
//  class SortedList(object, UserDict.DictMixin):
//    def __init__(self, table):
//      self.sorted_list = make_sorted_list(table)
//  
//    def __len__(self):
//      return len(self.sorted_list[0])
//  
//    def __getitem__(self, key):
//      retval = lookup_sorted_list(self.sorted_list, key)
//      if retval is None:
//        raise KeyError(key)
//      return retval
//  
//    def __contains__(self, key):
//      return lookup_sorted_list(self.sorted_list, key) is not None
//  
//    def __iter__(self):
//      (keys, values) = self.sorted_list
//      for x in keys:
//        yield x
//  
//    def keys(self):
//      return self.sorted_list[0]
//  
//    def itervalues(self):
//      (keys, values) = self.sorted_list
//      for x in values:
//        yield x
//  
//    def iteritems(self):
//      (keys, values) = self.sorted_list
//      for (key, value) in izip(keys, values):
//        yield (key, value)
//
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
    table: Map[T,U],
    outfile: PrintStream=System.out, indent: String="",
    keep_secondary_order: Boolean=false, maxrows: Int = -1) {
    output_reverse_sorted_list(table toList)
  }

  /////////////////////////////////////////////////////////////////////////////
  //                             Metered Tasks                               //
  /////////////////////////////////////////////////////////////////////////////

  /**
   * Class for tracking number of items processed in a long task, and
   * reporting periodic status messages concerning how many items
   * processed and how much real time involved.  Call `item_processed`
   * every time you have processed an item.
   *
   * @param item_name Generic name of the items being processed, for the
   *    status messages
   * @param verb Transitive verb in its -ing form indicating what is being
   *    done to the items
   * @param secs_between_output Number of elapsed seconds between successive
   *    periodic status messages
   */
  class MeteredTask(item_name: String, verb: String,
    secs_between_output: Double = 15) {
    val plural_item_name = pluralize(item_name)
    var items_processed = 0
    // Whether we've already printed stats after the most recent item
    // processed
    var printed_stats = false
    errprint("--------------------------------------------------------")
    val first_time = curtimesecs()
    var last_time = first_time
    errprint("Beginning %s %s at %s.", verb, plural_item_name,
      humandate_full(first_time))
    errprint("")
  
    def num_processed() = items_processed
  
    def elapsed_time() = curtimesecs() - first_time
  
    def item_unit() = {
      if (items_processed == 1)
        item_name
      else
        plural_item_name
    }
 
    def print_elapsed_time_and_rate(curtime: Double = curtimesecs(),
        nohuman: Boolean = false) {
      /* Don't do anything if already printed for this item. */
      if (printed_stats)
        return
      printed_stats = true
      val total_elapsed_secs = curtime - first_time
      val attime =
        if (nohuman) "" else "At %s: " format humandate_time(curtime) 
      errprint("%sElapsed time: %s minutes %s seconds, %s %s processed",
               attime,
               (total_elapsed_secs / 60).toInt,
               (total_elapsed_secs % 60).toInt,
               items_processed, item_unit())
      val items_per_second = items_processed.toDouble / total_elapsed_secs
      val seconds_per_item = total_elapsed_secs / items_processed
      errprint("Processing rate: %s items per second (%s seconds per item)",
               format_float(items_per_second),
               format_float(seconds_per_item))
    }

    def item_processed(maxtime: Double = 0.0) = {
      val curtime = curtimesecs()
      items_processed += 1
      val total_elapsed_secs = curtime - first_time
      val last_elapsed_secs = curtime - last_time
       printed_stats = false
      if (last_elapsed_secs >= secs_between_output) {
        // Rather than directly recording the time, round it down to the
        // nearest multiple of secs_between_output; else we will eventually
        // see something like 0, 15, 45, 60, 76, 91, 107, 122, ...
        // rather than like 0, 15, 45, 60, 76, 90, 106, 120, ...
        val rounded_elapsed =
          ((total_elapsed_secs / secs_between_output).toInt *
           secs_between_output)
        last_time = first_time + rounded_elapsed
        print_elapsed_time_and_rate(curtime)
      }
      if (maxtime > 0 && total_elapsed_secs >= maxtime) {
        errprint("Maximum time reached, interrupting processing")
        print_elapsed_time_and_rate(curtime)
        true
      }
      else false
    }

    /**
     * Output a message indicating that processing is finished, along with
     * stats given number of items processed, time, and items/time, time/item.
     * The total message looks like "Finished _doing_ _items_." where "doing"
     * comes from the `doing` parameter to this function and should be a
     * lower-case transitive verb in the -ing form.  The actual value of
     * "items" comes from the `item_name` constructor parameter to this
     * class. */ 
    def finish() = {
      val curtime = curtimesecs()
      errprint("")
      errprint("Finished %s %s at %s.", verb, plural_item_name,
        humandate_full(curtime))
      print_elapsed_time_and_rate(curtime, nohuman = true)
      errprint("--------------------------------------------------------")
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

//  //////////////////////////////////////////////////////////////////////////////
//  //                               Priority Queues                            //
//  //////////////////////////////////////////////////////////////////////////////
//
//  // Priority queue implementation, based on Python heapq documentation.
//  // Note that in Python 2.6 and on, there is a priority queue implementation
//  // in the Queue module.
//  class PriorityQueue(object):
//    INVALID = 0                     // mark an entry as deleted
//  
//    def __init__(self):
//      self.pq = []                         // the priority queue list
//      self.counter = itertools.count(1)    // unique sequence count
//      self.task_finder = {}                // mapping of tasks to entries
//  
//    def add_task(self, priority, task, count=None):
//      if count is None:
//        count = self.counter.next()
//      entry = [priority, count, task]
//      self.task_finder[task] = entry
//      heappush(self.pq, entry)
//  
//    //Return the top-priority task. If 'return_priority' is false, just
//    //return the task itself; otherwise, return a tuple (task, priority).
//    def get_top_priority(self, return_priority=false):
//      while true:
//        priority, count, task = heappop(self.pq)
//        if count is not PriorityQueue.INVALID:
//          del self.task_finder[task]
//          if return_priority:
//            return (task, priority)
//          else:
//            return task
//  
//    def delete_task(self, task):
//      entry = self.task_finder[task]
//      entry[1] = PriorityQueue.INVALID
//  
//    def reprioritize(self, priority, task):
//      entry = self.task_finder[task]
//      self.add_task(priority, task, entry[1])
//      entry[1] = PriorityQueue.INVALID
//
  ///////////////////////////////////////////////////////////////////////////
  //                    Least-recently-used (LRU) Caches                   //
  ///////////////////////////////////////////////////////////////////////////

  class LRUCache[T,U](maxsize: Int=1000) extends mutable.Map[T,U]
    with mutable.MapLike[T,U,LRUCache[T,U]] {
    val cache = mutable.LinkedHashMap[T,U]()

    // def length = return cache.length

    private def reprioritize(key: T) {
      val value = cache(key)
      cache -= key
      cache(key) = value
    }

    def get(key: T): Option[U] = {
      if (cache contains key) {
        reprioritize(key)
        Some(cache(key))
      }
      else None
    }
 
    override def update(key: T, value: U) {
      if (cache contains key)
        reprioritize(key)
      else {
        while (cache.size >= maxsize) {
          val (key2, value) = cache.head
          cache -= key2
        }
        cache(key) = value
      }
    }

    override def remove(key: T): Option[U] = cache.remove(key)
 
    def iterator: Iterator[(T, U)] = cache.iterator

    // All the rest Looks like pure boilerplate!  Why necessary?
    def += (kv: (T, U)): this.type = {
      update(kv._1, kv._2); this }
    def -= (key: T): this.type = { remove(key); this }

    override def empty = new LRUCache[T,U]()
    }

  // This whole object looks like boilerplate!  Why necessary?
  object LRUCache extends {
    def empty[T,U] = new LRUCache[T,U]()

    def apply[T,U](kvs: (T,U)*): LRUCache[T,U] = {
      val m: LRUCache[T,U] = empty
      for (kv <- kvs) m += kv
       m
    }

    def newBuilder[T,U]: Builder[(T,U), LRUCache[T,U]] =
      new MapBuilder[T, U, LRUCache[T,U]](empty)

    implicit def canBuildFrom[T,U]
      : CanBuildFrom[LRUCache[T,U], (T,U), LRUCache[T,U]] =
        new CanBuildFrom[LRUCache[T,U], (T,U), LRUCache[T,U]] {
          def apply(from: LRUCache[T,U]) = newBuilder[T,U]
          def apply() = newBuilder[T,U]
        }
  }

  ////////////////////////////////////////////////////////////////////////////
  //                             Resource Usage                             //
  ////////////////////////////////////////////////////////////////////////////

  /**
    * Return floating-point value, number of seconds since the Epoch
    **/
  def curtimesecs() = System.currentTimeMillis()/1000.0

  def curtimehuman() = (new Date()) toString

  def humandate_full(sectime: Double) =
    (new Date((sectime*1000).toLong)) toString
  def humandate_time(sectime: Double) =
    DateFormat.getTimeInstance().format((sectime*1000).toLong)

  import java.lang.management._
  def getpid() = ManagementFactory.getRuntimeMXBean().getName().split("@")(0)
  val beginning_prog_time = curtimesecs()
  
  def get_program_time_usage() = curtimesecs() - beginning_prog_time

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
    for (line <- openr("/proc/self/status")) {
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
        errprint("%s bytes", long_with_commas(mem))
    }
  }

  def output_resource_usage(dojava: Boolean = true) {
    errprint("Total elapsed time since program start: %s",
             format_minutes_seconds(get_program_time_usage()))
    val (vszmeth, vsz) = get_program_memory_usage(virtual = true,
      method = "auto")
    errprint("Memory usage, virtual memory size (%s): %s bytes", vszmeth,
      long_with_commas(vsz))
    val (rssmeth, rss) = get_program_memory_usage(virtual = false,
      method = "auto")
    errprint("Memory usage, actual (i.e. resident set) (%s): %s bytes", rssmeth,
      long_with_commas(rss))
    if (dojava) {
      val (_, java) = get_program_memory_usage(virtual = false,
        method = "java")
      errprint("Memory usage, Java heap: %s bytes", long_with_commas(java))
    } else
      System.gc()
  }

  ////////////////////////////////////////////////////////////////////////////
  //                           Hash tables by range                         //
  ////////////////////////////////////////////////////////////////////////////

  // A table that groups all keys in a specific range together.  Instead of
  // directly storing the values for a group of keys, we store an object (termed a
  // "collector") that the user can use to keep track of the keys and values.
  // This way, the user can choose to use a list of values, a set of values, a
  // table of keys and values, etc.
  
  // 'ranges' is a sorted list of numbers, indicating the
  // boundaries of the ranges.  One range includes all keys that are
  // numerically below the first number, one range includes all keys that are
  // at or above the last number, and there is a range going from each number
  // up to, but not including, the next number.  'collector' is used to create
  // the collectors used to keep track of keys and values within each range;
  // it is either a type or a no-argument factory function.  We only create
  // ranges and collectors as needed. 'lowest_bound' is the value of the
  // lower bound of the lowest range; default is 0.  This is used only
  // it iter_ranges() when returning the lower bound of the lowest range,
  // and can be an item of any type, e.g. the number 0, the string "-infinity",
  // etc.
  abstract class TableByRange[Coll,Numtype <% Ordered[Numtype]](
    ranges: Seq[Numtype],
    create: ()=>Coll
  ) {
    val min_value: Numtype
    val max_value: Numtype
    val items_by_range = mutable.Map[Numtype,Coll]()
    var seen_negative = false
  
    def get_collector(key: Numtype) = {
      // This somewhat scary-looking cast produces 0 for Int and 0.0 for
      // Double.  If you write it as 0.asInstanceOf[Numtype], you get a
      // class-cast error when < is called if Numtype is Double because the
      // result of the cast ends up being a java.lang.Integer which can't
      // be cast to java.lang.Double. (FMH!!!)
      if (key < null.asInstanceOf[Numtype])
        seen_negative = true
      var lower_range = min_value
      // upper_range = "infinity"
      breakable {
        for (i <- ranges) {
          if (i <= key)
            lower_range = i
          else {
            // upper_range = i
            break
          }
        }
      }
      if (!(items_by_range contains lower_range))
        items_by_range(lower_range) = create()
      items_by_range(lower_range)
    }
  
    /**
     Return an iterator over ranges in the table.  Each returned value is
     a tuple (LOWER, UPPER, COLLECTOR), giving the lower and upper bounds
     (inclusive and exclusive, respectively), and the collector item for this
     range.  The lower bound of the lowest range comes from the value of
     'lowest_bound' specified during creation, and the upper bound of the range
     that is higher than any numbers specified during creation in the 'ranges'
     list will be the string "infinity" if such a range is returned.
  
     The optional arguments 'unseen_between' and 'unseen_all' control the
     behavior of this iterator with respect to ranges that have never been seen
     (i.e. no keys in this range have been passed to 'get_collector').  If
     'unseen_all' is true, all such ranges will be returned; else if
     'unseen_between' is true, only ranges between the lowest and highest
     actually-seen ranges will be returned.
     */
    def iter_ranges(unseen_between: Boolean=true, unseen_all: Boolean=false) = {
      var highest_seen: Numtype = 0.asInstanceOf[Numtype]
      val iteration_range =
        (List(if (seen_negative) min_value else 0.asInstanceOf[Numtype]) ++
          ranges) zip
         (ranges ++ List(max_value))
      for ((lower, upper) <- iteration_range) {
        if (items_by_range contains lower)
          highest_seen = upper
      }
  
      var seen_any = false
      for {(lower, upper) <- iteration_range
           // FIXME SCALABUG: This is a bug in Scala if I have to do this
           val collector = items_by_range.getOrElse(lower, null.asInstanceOf[Coll])
           if (collector != null || unseen_all ||
               (unseen_between && seen_any &&
                upper != max_value && upper <= highest_seen))
           val col2 = if (collector != null) collector else create()
          }
      yield {
        if (collector != null) seen_any = true
        (lower, upper, col2)
      }
    }
  }

  class IntTableByRange[Coll](
    ranges: Seq[Int],
    create: ()=>Coll
  ) extends TableByRange[Coll,Int](ranges, create) {
    val min_value = java.lang.Integer.MIN_VALUE
    val max_value = java.lang.Integer.MAX_VALUE
  }

  class DoubleTableByRange[Coll](
    ranges: Seq[Double],
    create: ()=>Coll
  ) extends TableByRange[Coll,Double](ranges, create) {
    val min_value = java.lang.Double.NEGATIVE_INFINITY
    val max_value = java.lang.Double.POSITIVE_INFINITY
  }

 
//  //////////////////////////////////////////////////////////////////////////////
//  //                          Depth-, breadth-first search                    //
//  //////////////////////////////////////////////////////////////////////////////
//
//  // General depth-first search.  'node' is the node to search, the top of a
//  // tree.  'matches' indicates whether a given node matches.  'children'
//  // returns a list of child nodes.
//  def depth_first_search(node, matches, children):
//    nodelist = [node]
//    while len(nodelist) > 0:
//      node = nodelist.pop()
//      if matches(node):
//        yield node
//      nodelist.extend(reversed(children(node)))
//  
//  // General breadth-first search.  'node' is the node to search, the top of a
//  // tree.  'matches' indicates whether a given node matches.  'children'
//  // returns a list of child nodes.
//  def breadth_first_search(node, matches, children):
//    nodelist = deque([node])
//    while len(nodelist) > 0:
//      node = nodelist.popLeft()
//      if matches(node):
//        yield node
//      nodelist.extend(children(node))
//

 /////////////////////////////////////////////////////////////////////////////
 //                        Misc. list/iterator functions                    //
 /////////////////////////////////////////////////////////////////////////////

  /**
   *  Return the median value of a list.  List will be sorted, so this is O(n).
   */
  def median(list: Seq[Double]) = {
    val sorted = list.sorted
    val len = sorted.length
    if (len % 2 == 1)
      sorted(len / 2)
    else {
      val midp = len / 2
      0.5*(sorted(midp-1) + sorted(midp))
    }
  }
  
  /**
   *  Return the mean of a list.
   */
  def mean(list: Seq[Double]) = {
    list.sum / list.length
  }
  
  def fromto(from: Int, too: Int) = {
    if (from <= too) (from to too)
    else (too to from)
  }

//  // Return an iterator over all elements in all the given sequences, omitting
//  // elements seen more than once and keeping the order.
//  def merge_sequences_uniquely(*seqs):
//    table = {}
//    for seq in seqs:
//      for s in seq:
//        if s not in table:
//          table[s] = true
//          yield s
//
//  
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

/* For testing the output_memory_usage() function. */
object TestMemUsage extends App {
  tgutil.output_memory_usage()
}
