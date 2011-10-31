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
import org.clapper.argot._
import util.control.Breaks._
import collection.mutable

/**
  This module implements an argument parser for Scala, which handles
  both options (e.g. --output-file foo.txt) and positional parameters.
  It is built on top of Argot and has an interface that is designed to
  be quite similar to the argument-parsing mechanisms in Python.

  The parser tries to be easy to use, and in particular to emulate the
  field-based method of accessing values used in Python.  This leads to
  the need to be very slightly tricky in the way that arguments are
  declared; see below.

  The basic features here that Argot doesn't have are:

  (1) Argument specification is simplified through the use of optional
      parameters to the argument-creation functions.
  (2) A simpler and easier-to-use interface is provided for accessing
      argument values given on the command line, so that the values can be
      accessed as simple field references (re-assignable if needed).
  (3) Default values can be given.
  (4) "Choice"-type arguments can be specified (only one of a fixed number
      of choices allowed).  This can include multiple aliases for a given
      choice.
  (5) The "flag" type is simplified to just handle boolean flags, which is
      the most common usage. (FIXME: Eventually perhaps we should consider
      also allowing more general Argot-type flags.)
  (6) A reasonable default value is provided for the "meta-variable"
      parameter for options (which specifies the type of the argument).
  (7) Conversion functions are easier to specify, since one function suffices
      for all types of arguments.
  
  In general, to parse arguments, you first create an object of type
  ArgParser (call it `ap`), and then add options to it by calling
  functions, typically:

  -- ap.option[T]() for a single-valued option of type T
  -- ap.multiOption[T]() for a multi-valued option of type T (i.e. the option
       can be specified multiple times on the command line, and all such
       values will be accumulated into a List)
  -- ap.flag() for a boolean flag
  -- ap.parameter[T]() for a positional parameter (coming after all options)
  -- ap.multiParameter[T]() for a multi-valued positional parameter (i.e.
     eating up any remaining positional parameters given)

  There are two styles for accessing the values of arguments specified on the
  command line.  One possibility is to simply declare arguments by calling the
  above functions, then parse a command line using `ap.parse()`, then retrieve
  values using `ap.get[T]()` to get the value of a particular argument.

  However, this style is not as convenient as we'd like, especially since
  the type must be specified.  In the original Python API, once the
  equivalent calls have been made to specify arguments and a command line
  parsed, the argument values can be directly retrieved from the ArgParser
  object as if they were fields; e.g. if an option `--outfile` were
  declared using a call like `ap.option[String]("outfile", ...)`, then
  after parsing, the value could simply be fetched using `ap.outfile`,
  and assignment to `ap.outfile` would be possible, e.g. if the value
  is to be defaulted from another argument.

  This functionality depends on the ability to dynamically intercept
  field references and assignments, which doesn't currently exist in
  Scala.  However, it is possible to achieve a near-equivalent.  It works
  like this:

  1) Functions like `ap.option[T]()` are set up so that the first time they
     are called for a given ArgParser object and argument, they will note
     the argument, and return the default value of this argument.  If called
     again after parsing, however, they will return the value specified in
     the command line (or the default if no value was specified). (If called
     again *before* parsing, they simply return the default value, as before.)

  2) A class, e.g. ProgArgs, is created to hold the values returned from
     the command line.  This class typically looks like this:
     
     class ProgArgs(ap: ArgParser) {
       var outfile = ap.option[String]("outfile", "o", ...)
       var verbose = ap.flag("verbose", "v", ...)
       ...
     }

  3) To parse a command line, we proceed as follows:

     a) Create an ArgParser object.
     b) Create an instance of ProgArgs, passing in the ArgParser object.
     c) Call `parse()` on the ArgParser object, to parse a command line.
     d) Create *another* instance of ProgArgs, passing in the *same*
        ArgParser object.
     e) Now, the argument values specified on the command line can be
        retrieved from this new instance of ProgArgs simply using field
        accesses, and new values can likewise be set using field accesses.

  Note how this actually works.  When the first instance of ProgArgs is
  created, the initialization of the variables causes the arguments to be
  specified on the ArgParser -- and the variables have the default values
  of the arguments.  When the second instance is created, after parsing, and
  given the *same* ArgParser object, the respective calls to "initialize"
  the arguments have no effect, but now return the values specified on the
  command line.  Because these are declared as `var`, they can be freely
  re-assigned.  Furthermore, because no actual reflection or any such thing
  is done, the above scheme will work completely fine if e.g. ProgArgs
  subclasses another class that also declares some arguments (e.g. to
  abstract out common arguments for multiple applications).  In addition,
  there is no problem mixing and matching the scheme described here with the
  conceptually simpler scheme where argument values are retrieved using
  `ap.get[T]()`.
 */

package object argparser {
  /*

  NOTE: At one point, in place of the second scheme described above, there
  was a scheme involving reflection.  This didn't work as well, and ran
  into various problems.  One such problem is described here, because it
  shows some potential limitations/bugs in Scala.  In particular, in that
  scheme, calls to `ap.option[T]()` and similar were declared using `def`
  instead of `var`, and the first call to them was made using reflection.
  Underlyingly, all defs, vars and vals look like functions, and fields
  declared as `def` simply look like no-argument functions.  Because the
  return type can vary and generally is a simple type like Int or String,
  there was no way to reliably recognize defs of this sort from other
  variables, functions, etc. in the object.  To make this recognition
  reliable, I tried wrapping the return value in some other object, with
  bidirectional implicit conversions to/from the wrapped value, something
  like this:
      
  class ArgWrap[T](vall: T) extends ArgAny[T] {
    def value = vall
    def specified = true
  }

  implicit def extractValue[T](arg: ArgAny[T]): T = arg.value

  implicit def wrapValue[T](vall: T): ArgAny[T] = new ArgWrap(vall)

  Unfortunately, this didn't work, for somewhat non-obvious reasons.
  Specifically, the problems were:

  (1) Type unification between wrapped and unwrapped values fails.  This is
      a problem e.g. if I try to access a field value in an if-then-else
      statements like this, I run into problems:

      val files =
        if (use_val)
          Args.files
        else
          Seq[String]()

      This unifies to AnyRef, not Seq[String], even if Args.files wraps
      a Seq[String].

  (2) Calls to methods on the wrapped values (e.g. strings) can fail in
      weird ways.  For example, the following fails:

      def words = ap.option[String]("words")

      ...

      val split_words = ap.words.split(',')

      However, it doesn't fail if the argument passed in is a string rather
      than a character.  In this case, if I have a string, I *can* call
      split with a character as an argument - perhaps this fails in the case
      of an implicit conversion because there is a split() implemented on
      java.lang.String that takes only strings, whereas split() that takes
      a character is stored in StringOps, which itself is handled using an
      implicit conversion.
   */

  implicit def convertInt(rawval: String, arg: ArgSingle[Int]) = {
    try { rawval.toInt }
    catch {
      case e: NumberFormatException =>
        throw new ArgParserConversionException(
          """Cannot convert argument "%s" to an integer.""" format rawval)
    }
  }

  implicit def convertDouble(rawval: String, arg: ArgSingle[Double]) = {
    try { rawval.toDouble }
    catch {
      case e: NumberFormatException =>
        throw new ArgParserConversionException(
          """Cannot convert argument "%s" to a floatig-point number."""
          format rawval)
    }
  }

  implicit def convertString(rawval: String, arg: ArgSingle[String]) = {
    rawval
  }

  implicit def convertBoolean(rawval: String, arg: ArgSingle[Boolean]) = {
    rawval.toLowerCase match {
      case "yes" => true
      case "no" => false
      case "true" => true
      case "false" => false
      case "t" => true
      case "f" => false
      case "on" => true
      case "off" => false
      case _ => throw new ArgParserConversionException(
          ("""Cannot convert argument "%s" to a boolean.  """ +
           """Recognized values (case-insensitive) are """ +
           """yes, no, true, false, t, f, on, off.""") format rawval)
    }
  }

  class ArgParserException(val message: String,
    val cause: Option[Throwable] = None) extends Exception(message) {
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
   * Thrown to indicate usage errors. The calling application can catch this
   * exception and print the message, which will be a fully fleshed-out usage
   * string. For instance:
   *
   * (FIXME: Not implemented currently.)
   *
   * {{{
   * import opennlp.textgrounder.geolocate.argparser._
   *
   * object TestArgs extends App {
   *   class Args {
   *     var foo = ap.option[Int]("foo", default=5)
   *     var bar = ap.option[String]("bar", default="chinga")
   *     var baz = ap.multiOption[String]("baz")
   *     var bat = ap.multiOption[Int]("bat")
   *     var blop = ap.option[String]("blop", choices=Seq("mene", "tekel", "upharsin"))
   *     var blop2 = ap.multiOption[String]("blop2", choices=Seq("mene", "tekel", "upharsin"))
   *   }
   *   ...
   *   val ap = new ArgParser("test")
   *   new Args(ap)    // necessary!
   *   ap.parse(args)
   *   val args = new Args(ap)
   *   ...
   *   println("foo: %s" format args.foo)
   *   println("bar: %s" format args.bar)
   *   println("baz: %s" format args.baz)
   *   println("bat: %s" format args.bat)
   *   println("blop: %s" format args.blop)
   *   println("blop2: %s" format args.blop2)
   * }
   *
   * val p = new ArgParser("MyProgram")
   * ...
   * try {
   *   p.parse(args)
   * }
   * catch {
   *   case e: ArgParserUsageException =>
   *     println(e.message)
   *     System.exit(1)
   * }
   * }}}
   *
   * @param message  exception message
   */
  class ArgParserUsageException(message: String)
    extends ArgParserException(message)

  /**
   * Thrown to indicate that ArgParser could not convert a command line
   * parameter to the desired type.
   *
   * @param message  exception message
   */
  class ArgParserConversionException(message: String)
    extends ArgParserException(message)

  /**
   * Thrown to indicate that ArgParser encountered a problem in the caller's
   * argument specification, or something else indicating invalid coding.
   * This indicates a big in the caller's code.
   *
   * @param message  exception message
   */
  class ArgParserCodingError(message: String,
    cause: Option[Throwable] = None
  ) extends ArgParserException("(CALLER BUG) " + message, cause)

  /**
   * Thrown to indicate that ArgParser encountered a problem that should
   * never occur under any circumstances, indicating a bug in the ArgParser
   * code itself.
   *
   * @param message  exception message
   */
  class ArgParserInternalError(message: String,
    cause: Option[Throwable] = None
  ) extends ArgParserException("(INTERNAL BUG) " + message, cause)

  class ArgParserInvalidChoiceException(message: String)
    extends ArgParserConversionException(message)

  object ArgParser {
    def canonName(name: Seq[String]): String = {
      assert(name.length > 0)
      for (n <- name) {
        if (n.length > 1) return n
      }
      return name(0)
    }

    def computeMetavar(metavar: String, name: Seq[String]) = {
      if (metavar != null) metavar
      else canonName(name).toUpperCase
    }

    def nonNullVals(val1: String, val2: String, val3: String, val4: String,
      val5: String, val6: String, val7: String, val8: String,
      val9: String) = {
      val retval =
        Seq(val1, val2, val3, val4, val5, val6, val7, val8, val9) filter
          (_ != null)
      if (retval.length == 0)
        throw new ArgParserCodingError(
          "Need to specify at least one name for each argument")
      retval
    }

    def checkChoices[T](converted: T, choices: Seq[T],
      canon: Map[T, Iterable[T]]) = {
      var retval = converted
      var recanon: Map[T, T] = null
      if (canon != null) {
        // Convert to a map in the other direction
        recanon =
          for ((full, abbrevs) <- canon; abbrev <- abbrevs)
            yield (abbrev -> full)
        retval = recanon.getOrElse(retval, retval)
      }
      if (choices == null || (choices contains retval)) retval
      else {
        // Mapping for all choices, including non-canonical versions
        val allchoices = mutable.Set[String]()
        for (c <- choices)
          allchoices += c.toString
        if (recanon != null)
          for ((abbrev, full) <- recanon)
            allchoices += "%s(=%s)" format (abbrev, full)
        throw new ArgParserInvalidChoiceException("Choice '%s' not one of the recognized choices: %s" format (retval, allchoices mkString ", "))
      }
    }
  }

  abstract class ArgAny[T](name: String) {
    def value: T
    def apply() = value
    def specified: Boolean
  }

  class ArgFlag(
    val name: String
  ) extends ArgAny[Boolean](name) {
    var wrap: FlagOption[Boolean] = null
    def value = {
      wrap.value match {
        case Some(x) => x
        case None => false
      }
    }
    def specified = (wrap != null && wrap.value != None)
  }

  class ArgSingle[T](
    val default: T,
    val name: String,
    val is_param: Boolean = false
  ) extends ArgAny[T](name) {
    var wrap: SingleValueArg[T] = null
    def value = {
      wrap.value match {
        case Some(x) => x
        case None => default
      }
    }
    def specified = (wrap != null && wrap.value != None)
  }

  class ArgMulti[T](
    val default: Seq[T],
    val name: String,
    val is_param: Boolean = false
  ) extends ArgAny[Seq[T]](name) {
    var wrap: MultiValueArg[T] = null
    val wrapSingle = new ArgSingle[T](null.asInstanceOf[T], name)
    def value = if (wrap.value.length == 0) default else wrap.value
    def specified = (wrap.value.length > 0)
  }

  class ArgParser(prog: String, return_defaults: Boolean = false) {
    import ArgParser._
    import ArgotConverters._
    protected val argot = new ArgotParser(prog)
    /* A map from the parameter's canonical name to the subclass of ArgAny
       describing the parameter and holding its value.  The canonical name
       of options comes from the first non-single-letter name.  The
       canonical name of positional parameters is simply the name of the
       parameter.  Iteration over the map yields keys in the order they
       were added rather than random. */
    protected val argmap = mutable.LinkedHashMap[String, ArgAny[_]]()
    /* The type of each argument.  For multi options and parameters this will
       be of type Seq.  Because of type erasure, the type of sequence must
       be stored separately, using argtype_multi. */
    protected val argtype = mutable.Map[String, Class[_]]()
    /* For multi arguments, the type of each individual argument. */
    protected val argtype_multi = mutable.Map[String, Class[_]]()
    /* Set specifying arguments that are positional parameters. */
    protected val argpositional = mutable.Set[String]()
    /* Set specifying arguments that are flag options. */
    protected val argflag = mutable.Set[String]()

    /**
     * Return the value of an argument, or the default if not specified.
     *
     * @param arg The canonical name of the argument, i.e. the first
     *   non-single-letter alias given.
     * @returns The value, of type Any.
     */
    def apply(arg: String) = argmap(arg).value

    /**
     * Return the value of an argument, or the default if not specified.
     *
     * @param arg The canonical name of the argument, i.e. the first
     *   non-single-letter alias given.
     * @tparam T The type of the argument, which must match the type given
     *   in its definition
     *   
     * @returns The value, of type T.
     */
    def get[T](arg: String) = argmap(arg).asInstanceOf[ArgAny[T]].value

    /**
     * Return whether an argument (either option or positional parameter)
     * exists with the given canonical name.
     */
    def exists(arg: String) = argmap contains arg

    /**
     * Return whether an argument exists with the given canonical name.
     */
    def isOption(arg: String) = exists(arg) && !isPositional(arg)

    /**
     * Return whether a positional parameter exists with the given name.
     */
    def isPositional(arg: String) = argpositional contains arg

    /**
     * Return whether a flag option exists with the given canonical name.
     */
    def isFlag(arg: String) = argflag contains arg

    /**
     * Return whether a multi argument (either option or positional parameter)
     * exists with the given canonical name.
     */
    def isMulti(arg: String) = argtype_multi contains arg

    /**
     * Return whether the given argument's value was specified.
     */
    def specified(arg: String) = argmap(arg).specified

    /**
     * Return the type of the given argument.  For multi arguments, the
     * type will be Seq, and the type of the individual arguments can only
     * be retrieved using `getMultiType`, due to type erasure.
     */
    def getType(arg: String) = argtype(arg)

    /**
     * Return the type of an individual argument value of a multi argument.
     * The actual type of the multi argument is a Seq of the returned type.
     */
    def getMultiType(arg: String) = argtype_multi(arg)

    protected def handle_argument[T : Manifest, U : Manifest](name: Seq[String],
      default: U,
      metavar: String,
      create_underlying: (String, String) => ArgAny[U],
      is_multi: Boolean = false,
      is_positional: Boolean = false,
      is_flag: Boolean = false
    ) = {
      val canon = canonName(name)
      if (return_defaults)
        default
      else if (argmap contains canon)
        argmap(canon).asInstanceOf[ArgAny[U]].value
      else {
        val real_metavar = computeMetavar(metavar, name)
        val underobj = create_underlying(canon, real_metavar)
        argmap(canon) = underobj
        argtype(canon) = manifest[U].erasure
        if (is_multi)
          argtype_multi(canon) = manifest[T].erasure
        if (is_positional)
          argpositional += canon
        if (is_flag)
          argflag += canon
        default
      }
    }

    def optionSeq[T](name: Seq[String],
      default: T = null.asInstanceOf[T],
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, ArgSingle[T]) => T, m: Manifest[T]) = {
      def create_underlying(canon_name: String, real_metavar: String) = {
        val arg = new ArgSingle(default, canon_name)
        arg.wrap =
          argot.option[T](name.toList, real_metavar, help) {
            (rawval: String, argop: SingleValueOption[T]) =>
              {
                val converted = convert(rawval, arg)
                checkChoices(converted, choices, canonicalize)
              }
          }
        arg
      }
      handle_argument[T,T](name, default, metavar, create_underlying _)
    }

    /**
     * Define a single-valued option of type T.  Various standard types
     * are recognized, e.g. String, Int, Double. (This is handled through
     * the implicit `convert` argument.) Up to nine aliases for the
     * option can be specified.  Single-letter aliases are specified using
     * a single dash, whereas longer aliases generally use two dashes.
     * The "canonical" name of the option is the first non-single-letter
     * alias given.
     *
     * @param name1
     * @param name2
     * @param name3
     * @param name4
     * @param name5
     * @param name6
     * @param name7
     * @param name8
     * @param name9
     *    Up to nine aliases for the option; see above.
     * 
     * @param default Default value, if option not specified; if not given,
     *    it will end up as 0, 0.0 or false for value types, null for
     *    reference types.
     * @param metavar "Type" of the option, as listed in the usage string.
     *    This is so that the relevant portion of the usage string will say
     *    e.g. "--counts-file FILE     File containing word counts." (The
     *    value of `metavar` would be "FILE".) If not given, automatically
     *    computed from the canonical option name by capitalizing it.
     * @param choices List of possible choices for this option.  If specified,
     *    it should be a sequence, and only the specified choices will be
     *    allowed. (But see the `canonicalize` param below.) Otherwise, all
     *    values will be allowed.
     * @param canonicalize Mapping specifying aliases for choice values.
     *    The idea is that when there are a limited number of choices, there
     *    may be multiple aliases for a given choice, e.g. "dev", "devel" or
     *    "development".  If given, this should be a map, with the key a
     *    canonical choice and the value a sequence of aliases.  Note that
     *    if `canonicalize` is given but `choices` is not given, the list
     *    of possible choices is automatically derived from the keys of the
     *    `canonicalize` mapping.  To this end, choices with no aliases
     *    should be listed with `null` as the value instead of a sequence.
     * @param help Help string for the option, shown in the usage string.
     * @param convert Function to convert the raw option (a string) into
     *    a value of type `T`.  The second parameter given is an object
     *    giving some more information about the option (most notably,
     *    its canonical name, in case this is relevant to the conversion
     *    routine).  For standard types, this function does not need to be
     *    given.
     * @tparam T The type of the option.  For non-standard types, a
     *    converter must explicitly be given. (The standard types recognized
     *    are currently Int, Double, Boolean and String.)
     *
     * @returns If class parameter `return_defaults` is true, the default
     *    value.  Else, if the first time called, exits non-locally; this
     *    is used internally.  Otherwise, the value of the parameter.
     */
    def option[T](
      name1: String, name2: String = null, name3: String = null,
      name4: String = null, name5: String = null, name6: String = null,
      name7: String = null, name8: String = null, name9: String = null,
      default: T = null.asInstanceOf[T],
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, ArgSingle[T]) => T, m: Manifest[T]) = {
      optionSeq[T](nonNullVals(name1, name2, name3, name4, name5, name6,
        name7, name8, name9),
        metavar = metavar, default = default, choices = choices,
        canonicalize = canonicalize, help = help)(convert, m)
    }

    def flagSeq(name: Seq[String],
      help: String = "") = {
      import ArgotConverters._
      def create_underlying(canon_name: String, real_metavar: String) = {
        val arg = new ArgFlag(canon_name)
        arg.wrap = argot.flag[Boolean](name.toList, help)
        arg
      }
      handle_argument[Boolean,Boolean](name, false, null, create_underlying _)
    }

    /**
     * Define a boolean flag option.  Unlike other options, flags have no
     * associated value.  Instead, their type is always Boolean, with the
     * value 'true' if the flag is specified, 'false' if not.
     *
     * @param name1
     * @param name2
     * @param name3
     * @param name4
     * @param name5
     * @param name6
     * @param name7
     * @param name8
     * @param name9
     *    Up to nine aliases for the option; same as for `option[T]()`.
     * 
     * @param help Help string for the option, shown in the usage string.
     */
    def flag(name1: String, name2: String = null, name3: String = null,
      name4: String = null, name5: String = null, name6: String = null,
      name7: String = null, name8: String = null, name9: String = null,
      help: String = "") = {
      flagSeq(nonNullVals(name1, name2, name3, name4, name5, name6,
        name7, name8, name9),
        help = help)
    }

    def multiOptionSeq[T](name: Seq[String],
      default: Seq[T] = Seq[T](),
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, ArgSingle[T]) => T, m: Manifest[T]) = {
      def create_underlying(canon_name: String, real_metavar: String) = {
        val arg = new ArgMulti[T](default, canon_name)
        arg.wrap =
          argot.multiOption[T](name.toList, real_metavar, help) {
            (rawval: String, argop: MultiValueOption[T]) =>
              {
                val converted = convert(rawval, arg.wrapSingle)
                checkChoices(converted, choices, canonicalize)
              }
          }
        arg
      }
      handle_argument[T,Seq[T]](name, default, metavar, create_underlying _,
        is_multi = true)
    }

    /**
     * Specify an option that can be repeated multiple times.  The resulting
     * option value will be a sequence (Seq) of all the values given on the
     * command line (one value per occurrence of the option).  If there are
     * no occurrences of the option, the value will be an empty sequence.
     * (NOTE: This is different from single-valued options, where the
     * default value can be explicitly specified, and if not given, will be
     * `null` for reference types.  Here, `null` will never occur.)
     */
    def multiOption[T](
      name1: String, name2: String = null, name3: String = null,
      name4: String = null, name5: String = null, name6: String = null,
      name7: String = null, name8: String = null, name9: String = null,
      default: Seq[T] = Seq[T](),
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, ArgSingle[T]) => T, m: Manifest[T]) = {
      multiOptionSeq[T](nonNullVals(name1, name2, name3, name4, name5, name6,
        name7, name8, name9),
        default = default, metavar = metavar, choices = choices,
        canonicalize = canonicalize, help = help)(convert, m)
    }

    /**
     * Specify a positional parameter.  Positional parameters are processed
     * in order.  Optional parameters must occur after all non-optional
     * parameters.  The name of the parameter is only used in the usage file
     * and as the "name" parameter of the ArgSingle[T] object passed to
     * the (implicit) conversion routine.  Usually the name should be in
     * all caps.
     *
     * @see #multiParameter[T]
     */
    def parameter[T](name: String,
      default: T = null.asInstanceOf[T],
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "",
      optional: Boolean = false)
    (implicit convert: (String, ArgSingle[T]) => T, m: Manifest[T]) = {
      def create_underlying(canon_name: String, real_metavar: String) = {
        val arg = new ArgSingle(default, canon_name, is_param = true)
        arg.wrap =
          argot.parameter[T](name, help, optional) {
            // FUCK ME! Argot wants a Parameter[T], but this is private to
            // Argot.  So we have to pass in a superclass.
            (rawval: String, argop: CommandLineArgument[T]) =>
              {
                val converted = convert(rawval, arg)
                checkChoices(converted, choices, canonicalize)
              }
          }
        arg
      }
      handle_argument[T,T](Seq(name), default, null, create_underlying _,
        is_positional = true)
    }

    /**
     * Specify any number of positional parameters.  These must come after
     * all other parameters.
     *
     * @see #parameter[T].
     */
    def multiParameter[T](name: String,
      default: Seq[T] = Seq[T](),
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "",
      optional: Boolean = true)
    (implicit convert: (String, ArgSingle[T]) => T, m: Manifest[T]) = {
      def create_underlying(canon_name: String, real_metavar: String) = {
        val arg = new ArgMulti[T](default, canon_name, is_param = true)
        arg.wrap = argot.multiParameter[T](name, help, optional) {
            // FUCK ME! Argot wants a Parameter[T], but this is private to
            // Argot.  So we have to pass in a superclass.
            (rawval: String, argop: CommandLineArgument[T]) =>
              {
                val converted = convert(rawval, arg.wrapSingle)
                checkChoices(converted, choices, canonicalize)
              }
          }
        arg
      }
      handle_argument[T,Seq[T]](Seq(name), default, null, create_underlying _,
        is_multi = true, is_positional = true)
    }

    /**
     * Parse the given command-line arguments.  Extracted values of the
     * arguments can subsequently be obtained either using the `#get[T]`
     * function, by directly treating the ArgParser object as if it were
     * a hash table and casting the result, or by using a separate class
     * to hold the extracted values in fields, as described above.  The
     * last method is the recommended one and generally the easiest-to-
     * use for the consumer of the values.
     *
     * @param args Command-line arguments, from main() or the like
     */
    def parse(args: Seq[String]) = {
      if (argmap.size == 0)
        throw new ArgParserCodingError("No arguments initialized.  If you thought you specified arguments, you might have defined the corresponding fields with 'def' instead of 'var' or 'val'.")
       
      // println(argmap)
      argot.parse(args.toList)
    }

    def error(msg: String) {
      throw new ArgParserConversionException(msg)
    }

    def argNameObjects: Iterable[(String, ArgAny[_])] = {
      argmap.toIterable
    }

    def argNameValues: Iterable[(String, Any)] = {
      for ((name, argobj) <- argmap) yield (name, argobj.value)
    }
  }
}

object TestArgs extends App {
  import argparser._
  val ap = new ArgParser("test")
  class MyArgs(ap: ArgParser) {
    /* An integer option named --foo, with a default value of 5.  Can also
       be specified using --spam or -f. */
    var foo = ap.option[Int]("foo", "spam", "f", default = 5)
    /* A string option named --bar, with a default value of "chinga".  Can
       also be specified using -b. */
    var bar = ap.option[String]("bar", "b", default = "chinga")
    /* A string option named --baz, which can be given multiple times.
       Default value is an empty sequence. */
    var baz = ap.multiOption[String]("baz")
    /* A floating-point option named --bat, which can be given multiple times.
       Default value is the sequence Seq(2.5, 5.0, 7.5), which will obtain
       when the option hasn't been given at all. */
    var bat = ap.multiOption[Double]("bat", default = Seq(2.5, 5.0, 7.5))
    /* A flag --bezzaaf, alias -q.  Value is true if given, false if not. */
    var bezzaaf = ap.flag("bezzaaf", "q")
    /* An integer option --blop, with only the values 1, 2, 4 or 7 are
       allowed.  Default is 1.  Note, in this case, if the default is
       not given, it will end up as 0, even though this isn't technically
       a valid choice.  This could be considered a bug -- perhaps instead
       we should default to the first choice listed, or throw an error.
       (It could also be considered a possibly-useful hack to allow
       detection of when no choice is given; but this can be determined
       in a more reliable way using `ap.specified("blop")`.)
     */
    var blop = ap.option[Int]("blop", default = 1, choices = Seq(1, 2, 4, 7))
    /* A string option --daniel, with only the values "mene", "tekel", and
       "upharsin" allowed, but where values can be repeated, e.g.
       --daniel mene --daniel mene --daniel tekel --daniel upharsin
       . */
    var daniel = ap.multiOption[String]("daniel",
      choices = Seq("mene", "tekel", "upharsin"))
    /* A required positional parameter. */
    var destfile = ap.parameter[String]("DESTFILE",
      help = "Destination file to store output in")
    /* A multi-positional parameter that sucks up all remaining arguments. */
    var files = ap.multiParameter[String]("FILES", help = "Files to process")
  }
  // This first call is necessary, even though it doesn't appear to do
  // anything.  In particular, this ensures that all arguments have been
  // defined on `ap` prior to parsing.
  val first_shadow = new MyArgs(ap)
  // ap.parse(List("--foo", "7"))
  ap.parse(args)
  val Args = new MyArgs(ap)
  /* Print out values of all arguments, whether options or positional */
  for ((name, value) <- ap.argNameValues)
    println("%30s: %s" format (name, value))
}
