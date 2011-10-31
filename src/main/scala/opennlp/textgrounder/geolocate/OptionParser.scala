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
  An option parser for Scala, built on top of Argot and made to work
  quite similar to the option-parsing mechanisms in Python.

  The parser tries to be easy to use, similar to how options work in
  Python.  This leads to the need to be very slightly tricky in the way
  that options are declared; see below.

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
      parameter to options.
  (7) Conversion functions are easier to specify, since one function suffices
      for all types of arguments (whether single, multi, optional, positional,
      etc.).
  
  In general, to parse options, you first create an object of type
  OptionParser (call it `op`), and then add options to it by calling
  functions, typically:

  -- op.option[T]() for a single-valued option of type T
  -- op.multiOption[T]() for a multi-valued option of type T (i.e. the option
       can be specified multiple times on the command line, and all such
       values will be accumulated into a List)
  -- op.flag() for a boolean flag
  -- op.parameter[T]() for a positional parameter (coming after all options)
  -- op.multiParameter[T]() for a multi-valued positional parameter (i.e.
     eating up any remaining positional parameters given)

  There are two styles for accessing the values of arguments specified on the
  command line.  One possibility is to simply declare options by calling the
  above functions, then parse a command line using `op.parse()`, then retrieve
  values using `op.get[T]()` to get the value of a particular option.

  However, this style is not as convenient as we'd like, especially since
  the type must be specified.  In the original Python API, once the
  equivalent calls have been made to specify options and a command line
  parsed, the options can be directly retrieved from the OptionParser object
  as if they were fields; e.g. if an option `--outfile` were declared using
  a call like `op.option[String]("outfile", ...)`, then after parsing, the
  value could simply be fetched using `op.outfile`, and assignment to
  `op.outfile` would be possible, e.g. if the value is to be defaulted from
  another argument.
  
  This functionality depends on the ability to dynamically intercept field
  references and assignments, which doesn't currently exist in Scala.
  However, it is possible to achieve a near-equivalent.  It works like this:

  1) Functions like `op.option[T]()` are set up so that the first time they
     are called for a given OptionParser object and option, they will note
     the option, and return the default value of this option.  If called
     again after parsing, however, they will return the value specified in
     the command line (or the default if no value was specified). (If called
     again *before* parsing, they simply return the default value, as before.)

  2) A class, e.g. ProgOptions, is created to hold the values returned from
     the command line.  This class typically looks like this:
     
     class ProgOptions(op: OptionParser) {
       var outfile = op.option[String]("outfile", "o", ...)
       var verbose = op.flag("verbose", "v", ...)
       ...
     }

  3) To parse a command line, we proceed as follows:

     a) Create an OptionParser object.
     b) Create an instance of ProgOptions, passing in the OptionParser object.
     c) Call `parse()` on the OptionParser object, to parse a command line.
     d) Create *another* instance of ProgOptions, passing in the *same*
        OptionParser object.
     e) Now, the argument values specified on the command line can be
        retrieved from this new instance of ProgOptions simply using field
        accesses, and new values can likewise be set using field accesses.

  Note how this actually works.  When the first instance of ProgOptions is
  created, the initialization of the variables causes the arguments to be
  specified on the OptionParser -- and the variables have the default values
  of the arguments.  When the second instance is created, after parsing, and
  given the *same* OptionParser object, the respective calls to "initialize"
  the arguments have no effect, but now return the values specified on the
  command line.  Because these are declared as `var`, they can be freely
  re-assigned.  Furthermore, because no actual reflection or any such thing
  is done, the above scheme will work completely fine if e.g. ProgOptions
  subclasses another class that also declares some arguments (e.g. to
  abstract out common arguments for multiple applications).  In addition,
  there is no problem mixing and matching the scheme described here with the
  conceptually simpler scheme where argument values are retrieved using
  `op.get[T]()`.
 */

object OptParse {
  /*

  NOTE: At one point, in place of the second scheme described above, there
  was a scheme involving reflection.  This didn't work as well, and ran
  into various problems.  One such problem is described here, because it
  shows some potential limitations/bugs in Scala.  In particular, in that
  scheme, calls to `op.option[T]()` and similar were declared using `def`
  instead of `var`, and the first call to them was made using reflection.
  Underlyingly, all defs, vars and vals look like functions, and fields
  declared as `def` simply look like no-argument functions.  Because the
  return type can vary and generally is a simple type like Int or String,
  there was no way to reliably recognize defs of this sort from other
  variables, functions, etc. in the object.  To make this recognition
  reliable, I tried wrapping the return value in some other object, with
  bidirectional implicit conversions to/from the wrapped value, something
  like this:
      
  class OptionWrap[T](vall: T) extends OptionAny[T] {
    def value = vall
    def specified = true
  }

  implicit def extractValue[T](opt: OptionAny[T]): T = opt.value

  implicit def wrapValue[T](vall: T): OptionAny[T] = new OptionWrap(vall)

  Unfortunately, this didn't work, for somewhat non-obvious reasons.
  Specifically, the problems were:

  (1) Type unification between wrapped and unwrapped values fails.  This is
      a problem e.g. if I try to access a field value in an if-then-else
      statements like this, I run into problems:

      val files =
        if (use_val)
          Opts.files
        else
          Seq[String]()

      This unifies to AnyRef, not Seq[String], even if Opts.files wraps
      a Seq[String].

  (2) Calls to methods on the wrapped values (e.g. strings) can fail in
      weird ways.  For example, the following fails:

      def words = op.option[String]("words")

      ...

      val split_words = op.words.split(',')

      However, it doesn't fail if the argument passed in is a string rather
      than a character.  In this case, if I have a string, I *can* call
      split with a character as an argument - perhaps this fails in the case
      of an implicit conversion because there is a split() implemented on
      java.lang.String that takes only strings, whereas split() that takes
      a character is stored in StringOps, which itself is handled using an
      implicit conversion.
   */

  implicit def convertInt(rawval: String, op: OptionSingle[Int]) = {
    try { rawval.toInt }
    catch {
      case e: NumberFormatException =>
        throw new OptParseConversionException(
          """Cannot convert argument "%s" to an integer.""" format rawval)
    }
  }

  implicit def convertDouble(rawval: String, op: OptionSingle[Double]) = {
    try { rawval.toDouble }
    catch {
      case e: NumberFormatException =>
        throw new OptParseConversionException(
          """Cannot convert argument "%s" to a floatig-point number."""
          format rawval)
    }
  }

  implicit def convertString(rawval: String, op: OptionSingle[String]) = {
    rawval
  }

  implicit def convertBoolean(rawval: String, op: OptionSingle[Boolean]) = {
    rawval.toLowerCase match {
      case "yes" => true
      case "no" => false
      case "true" => true
      case "false" => false
      case "t" => true
      case "f" => false
      case "on" => true
      case "off" => false
      case _ => throw new OptParseConversionException(
          ("""Cannot convert argument "%s" to a boolean.  """ +
           """Recognized values (case-insensitive) are """ +
           """yes, no, true, false, t, f, on, off.""") format rawval)
    }
  }

  class OptParseException(val message: String,
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
   * import opennlp.textgrounder.geolocate.OptParse._
   *
   * object TestOpts extends App {
   *   val op = new OptionParser("test")
   *   object Opts {
   *     var foo = op.option[Int]("foo", default=5)
   *     var bar = op.option[String]("bar", default="fuck")
   *     var baz = op.multiOption[String]("baz")
   *     var bat = op.multiOption[Int]("bat")
   *     var blop = op.option[String]("blop", choices=Seq("mene", "tekel", "upharsin"))
   *     var blop2 = op.multiOption[String]("blop2", choices=Seq("mene", "tekel", "upharsin"))
   *   }
   *   ...
   *   op.parse(args)
   *   ...
   *   println("foo: %s" format Opts.foo)
   *   println("bar: %s" format Opts.bar)
   *   println("baz: %s" format Opts.baz)
   *   println("bat: %s" format Opts.bat)
   *   println("blop: %s" format Opts.blop)
   *   println("blop2: %s" format Opts.blop2)
   * }
   *
   * val p = new OptParse("MyProgram")
   * ...
   * try {
   *   p.parse(args)
   * }
   * catch {
   *   case e: OptParseUsageException =>
   *     println(e.message)
   *     System.exit(1)
   * }
   * }}}
   *
   * @param message  exception message
   */
  class OptParseUsageException(message: String)
    extends OptParseException(message)

  /**
   * Thrown to indicate that OptParse could not convert a command line
   * parameter to the desired type.
   *
   * @param message  exception message
   */
  class OptParseConversionException(message: String)
    extends OptParseException(message)

  /**
   * Thrown to indicate that OptParse encountered a problem in the caller's
   * argument specification, or something else indicating invalid coding.
   * This indicates a big in the caller's code.
   *
   * @param message  exception message
   */
  class OptParseCodingError(message: String,
    cause: Option[Throwable] = None
  ) extends OptParseException("(CALLER BUG) " + message, cause)

  /**
   * Thrown to indicate that OptParse encountered a problem that should
   * never occur under any circumstances, indicating a bug in the OptParse
   * code itself.
   *
   * @param message  exception message
   */
  class OptParseInternalError(message: String,
    cause: Option[Throwable] = None
  ) extends OptParseException("(INTERNAL BUG) " + message, cause)

  class OptParseInvalidChoiceException(message: String)
    extends OptParseConversionException(message)

  object OptionParser {
    def controllingOpt(opt: Seq[String]): String = {
      assert(opt.length > 0)
      for (o <- opt) {
        if (o.length > 1) return o
      }
      return opt(0)
    }

    def computeMetavar(metavar: String, opt: Seq[String]) = {
      if (metavar != null) metavar
      else controllingOpt(opt).toUpperCase
    }

    def nonNullVals(opt1: String, opt2: String, opt3: String, opt4: String,
      opt5: String, opt6: String, opt7: String, opt8: String,
      opt9: String) = {
      val retval =
        Seq(opt1, opt2, opt3, opt4, opt5, opt6, opt7, opt8, opt9) filter
          (_ != null)
      if (retval.length == 0)
        throw new OptParseCodingError(
          "Need to specify at least one command-line option")
      retval
    }

    def checkChoices[T](converted: T, choices: Seq[T],
      canon: Map[T, Iterable[T]]) = {
      var retval = converted
      if (canon != null) {
        // Convert to a map in the other direction
        val recanon =
          for ((full, abbrevs) <- canon; abbrev <- abbrevs)
            yield (abbrev -> full)
        retval = recanon.getOrElse(retval, retval)
      }
      if (choices == null || (choices contains retval)) retval
      else {
        // Mapping for all options, listing alternative 
        // FIXME: Implement this; should list choices along with non-canonical
        // versions of them
        val allopts = mutable.Map[String, mutable.Seq[String]]()

        throw new OptParseInvalidChoiceException("Choice '%s' not one of the recognized choices: %s" format (retval, choices))
      }
    }
  }

  abstract class OptionAny[T](name: String) {
    def value: T
    def apply() = value
    def specified: Boolean
  }

  class OptionFlag(
    val name: String
  ) extends OptionAny[Boolean](name) {
    var wrap: FlagOption[Boolean] = null
    def value = {
      wrap.value match {
        case Some(x) => x
        case None => false
      }
    }
    def specified = (wrap != null && wrap.value != None)
  }

  class OptionSingle[T](
    val default: T,
    val name: String,
    val is_param: Boolean = false
  ) extends OptionAny[T](name) {
    var wrap: SingleValueArg[T] = null
    def value = {
      wrap.value match {
        case Some(x) => x
        case None => default
      }
    }
    def specified = (wrap != null && wrap.value != None)
  }

  class OptionMulti[T](
    val default: Seq[T],
    val name: String,
    val is_param: Boolean = false
  ) extends OptionAny[Seq[T]](name) {
    var wrap: MultiValueArg[T] = null
    val wrapSingle = new OptionSingle[T](null.asInstanceOf[T], name)
    def value = if (wrap.value.length == 0) default else wrap.value
    def specified = (wrap.value.length > 0)
  }

  class OptionParser(prog: String,
      return_defaults: Boolean = false) {
    import OptionParser._
    import ArgotConverters._
    protected val op = new ArgotParser(prog)
    /* A map from the parameter's canonical name to the subclass of OptionAny
       describing the parameter and holding its value.  The canonical name
       of options comes from the "controlling option", i.e. first
       non-single-letter option alias.  The canonical name of positional
       parameters is simply the name of the parameter.  Iteration over the
       map yields keys in the order they were added rather than random. */
    protected val argmap = mutable.LinkedHashMap[String, OptionAny[_]]()
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
     * @param opt The canonical name of the argument, i.e. the first
     *   non-single-letter alias given.
     * @returns The value, of type Any.
     */
    def apply(opt: String) = argmap(opt).value

    /**
     * Return the value of an argument, or the default if not specified.
     *
     * @param opt The canonical name of the argument, i.e. the first
     *   non-single-letter alias given.
     * @tparam T The type of the argument, which must match the type given
     *   in its definition
     *   
     * @returns The value, of type T.
     */
    def get[T](opt: String) = argmap(opt).asInstanceOf[OptionAny[T]].value

    /**
     * Return whether an argument (either option or positional parameter)
     * exists with the given canonical name.
     */
    def exists(opt: String) = argmap contains opt

    /**
     * Return whether an option exists with the given canonical name.
     */
    def isOption(opt: String) = exists(opt) && !isPositional(opt)

    /**
     * Return whether a positional parameter exists with the given name.
     */
    def isPositional(opt: String) = argpositional contains opt

    /**
     * Return whether a flag option exists with the given canonical name.
     */
    def isFlag(opt: String) = argflag contains opt

    /**
     * Return whether a multi argument (either option or positional parameter)
     * exists with the given canonical name.
     */
    def isMulti(opt: String) = argtype_multi contains opt

    /**
     * Return whether the given argument's value was specified.
     */
    def specified(opt: String) = argmap(opt).specified

    /**
     * Return the type of the given argument.  For multi arguments, the
     * type will be Seq, and the type of the individual arguments can only
     * be retrieved using `getMultiType`, due to type erasure.
     */
    def getType(opt: String) = argtype(opt)

    /**
     * Return the type of an individual argument value of a multi argument.
     * The actual type of the multi argument is a Seq of the returned type.
     */
    def getMultiType(opt: String) = argtype_multi(opt)

    protected def handle_argument[T : Manifest, U : Manifest](opt: Seq[String],
      default: U,
      metavar: String,
      create_underlying: (String, String) => OptionAny[U],
      is_multi: Boolean = false,
      is_positional: Boolean = false,
      is_flag: Boolean = false
    ) = {
      val control = controllingOpt(opt)
      if (return_defaults)
        default
      else if (argmap contains control)
        argmap(control).asInstanceOf[OptionAny[U]].value
      else {
        val real_metavar = computeMetavar(metavar, opt)
        val option = create_underlying(control, real_metavar)
        argmap(control) = option
        argtype(control) = manifest[U].erasure
        if (is_multi)
          argtype_multi(control) = manifest[T].erasure
        if (is_positional)
          argpositional += control
        if (is_flag)
          argflag += control
        default
      }
    }

    def optionSeq[T](opt: Seq[String],
      default: T = null.asInstanceOf[T],
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, OptionSingle[T]) => T, m: Manifest[T]) = {
      def create_underlying(controlling_opt: String, real_metavar: String) = {
        val option = new OptionSingle(default, controlling_opt)
        option.wrap =
          op.option[T](opt.toList, real_metavar, help) {
            (rawval: String, op: SingleValueOption[T]) =>
              {
                val converted = convert(rawval, option)
                checkChoices(converted, choices, canonicalize)
              }
          }
        option
      }
      handle_argument[T,T](opt, default, metavar, create_underlying _)
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
     * @param opt1
     * @param opt2
     * @param opt3
     * @param opt4
     * @param opt5
     * @param opt6
     * @param opt7
     * @param opt8
     * @param opt9
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
      opt1: String, opt2: String = null, opt3: String = null,
      opt4: String = null, opt5: String = null, opt6: String = null,
      opt7: String = null, opt8: String = null, opt9: String = null,
      default: T = null.asInstanceOf[T],
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, OptionSingle[T]) => T, m: Manifest[T]) = {
      optionSeq[T](nonNullVals(opt1, opt2, opt3, opt4, opt5, opt6,
        opt7, opt8, opt9),
        metavar = metavar, default = default, choices = choices,
        canonicalize = canonicalize, help = help)(convert, m)
    }

    def flagSeq(opt: Seq[String],
      help: String = "") = {
      import ArgotConverters._
      def create_underlying(controlling_opt: String, real_metavar: String) = {
        val option = new OptionFlag(controlling_opt)
        option.wrap = op.flag[Boolean](opt.toList, help)
        option
      }
      handle_argument[Boolean,Boolean](opt, false, null, create_underlying _)
    }

    /**
     * Define a boolean flag option.  Unlike other options, flags have no
     * associated value.  Instead, their type is always Boolean, with the
     * value 'true' if the flag is specified, 'false' if not.
     *
     * @param opt1
     * @param opt2
     * @param opt3
     * @param opt4
     * @param opt5
     * @param opt6
     * @param opt7
     * @param opt8
     * @param opt9
     *    Up to nine aliases for the option; same as for `option[T]()`.
     * 
     * @param help Help string for the option, shown in the usage string.
     */
    def flag(opt1: String, opt2: String = null, opt3: String = null,
      opt4: String = null, opt5: String = null, opt6: String = null,
      opt7: String = null, opt8: String = null, opt9: String = null,
      help: String = "") = {
      flagSeq(nonNullVals(opt1, opt2, opt3, opt4, opt5, opt6,
        opt7, opt8, opt9),
        help = help)
    }

    def multiOptionSeq[T](opt: Seq[String],
      default: Seq[T] = Seq[T](),
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, OptionSingle[T]) => T, m: Manifest[T]) = {
      def create_underlying(controlling_opt: String, real_metavar: String) = {
        val option = new OptionMulti[T](default, controlling_opt)
        option.wrap =
          op.multiOption[T](opt.toList, real_metavar, help) {
            (rawval: String, op: MultiValueOption[T]) =>
              {
                val converted = convert(rawval, option.wrapSingle)
                checkChoices(converted, choices, canonicalize)
              }
          }
        option
      }
      handle_argument[T,Seq[T]](opt, default, metavar, create_underlying _,
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
      opt1: String, opt2: String = null, opt3: String = null,
      opt4: String = null, opt5: String = null, opt6: String = null,
      opt7: String = null, opt8: String = null, opt9: String = null,
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, OptionSingle[T]) => T, m: Manifest[T]) = {
      multiOptionSeq[T](nonNullVals(opt1, opt2, opt3, opt4, opt5, opt6,
        opt7, opt8, opt9),
        metavar = metavar, choices = choices,
        canonicalize = canonicalize, help = help)(convert, m)
    }

    /**
     * Specify a positional parameter.  Positional parameters are processed
     * in order.  Optional parameters must occur after all non-optional
     * parameters.  The name of the parameter is only used in the usage file
     * and as the "name" parameter of the OptionSingle[T] object passed to
     * the (implicit) conversion routine.  Usually the name should be in
     * all caps.
     *
     * @param
     *
     * @see #multiParameter[T]
     */
    def parameter[T](name: String,
      default: T = null.asInstanceOf[T],
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "",
      optional: Boolean = false)
    (implicit convert: (String, OptionSingle[T]) => T, m: Manifest[T]) = {
      def create_underlying(controlling_opt: String, real_metavar: String) = {
        val option = new OptionSingle(default, controlling_opt, is_param = true)
        option.wrap =
          op.parameter[T](name, help, optional) {
            // FUCK ME! Argot wants a Parameter[T], but this is private to
            // Argot.  So we have to pass in a superclass.
            (rawval: String, op: CommandLineArgument[T]) =>
              {
                val converted = convert(rawval, option)
                checkChoices(converted, choices, canonicalize)
              }
          }
        option
      }
      handle_argument[T,T](Seq(name), default, null, create_underlying _,
        is_positional = true)
    }

    /**
     * Specify any number of positional parameters.  These must come after
     * all other parameters.  @see parameter[T].
     */
    def multiParameter[T](name: String,
      default: Seq[T] = Seq[T](),
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "",
      optional: Boolean = true)
    (implicit convert: (String, OptionSingle[T]) => T, m: Manifest[T]) = {
      def create_underlying(controlling_opt: String, real_metavar: String) = {
        val option =
          new OptionMulti[T](default, controlling_opt, is_param = true)
        option.wrap =
          op.multiParameter[T](name, help, optional) {
            // FUCK ME! Argot wants a Parameter[T], but this is private to
            // Argot.  So we have to pass in a superclass.
            (rawval: String, op: CommandLineArgument[T]) =>
              {
                val converted = convert(rawval, option.wrapSingle)
                checkChoices(converted, choices, canonicalize)
              }
          }
        option
      }
      handle_argument[T,Seq[T]](Seq(name), default, null, create_underlying _,
        is_multi = true, is_positional = true)
    }

    /**
     * Parse the given command-line arguments, with `obj` containing defs
     * specifying options.  Values of the options can subsequently be
     * obtained simply by using the defs as fields.
     *
     * @param args Command-line arguments, from main() or the like
     */
    def parse(args: Seq[String]) = {
      if (argmap.size == 0)
        throw new OptParseCodingError("No arguments initialized.  If  you thought you specified arguments, you might have defined the corresponding fields with 'def' instead of 'var' or 'val'.")
       
      // println(argmap)
      op.parse(args.toList)
    }

    def error(msg: String) {
      throw new OptParseConversionException(msg)
    }

    def argNameObjects: Iterable[(String, OptionAny[_])] = {
      argmap.toIterable
    }

    def argNameValues: Iterable[(String, Any)] = {
      for ((name, optobj) <- argmap) yield (name, optobj.value)
    }
  }
}

object TestOpts extends App {
  import OptParse._
  val op = new OptionParser("test")
  class MyOpts(op: OptionParser) {
    /* An integer option named --foo, with a default value of 5.  Can also
       be specified using --spam or -f. */
    var foo = op.option[Int]("foo", "spam", "f", default = 5)
    /* A string option named --bar, with a default value of "chinga".  Can
       also be specified using -b. */
    var bar = op.option[String]("bar", "b", default = "chinga")
    /* A string option named --baz.  Default value is null. */
    var baz = op.multiOption[String]("baz")
    /* A floating-point option named --bat.  Default value is 0.0. */
    var bat = op.multiOption[Double]("bat")
    /* A flag --bezzaaf, alias -q.  Value is true if given, false if not. */
    var bezzaaf = op.flag("bezzaaf", "q")
    /* An integer option --blop, with only the values 1, 2, 4 or 7 are allowed.
       Default is 2. (Note, in this case, if the default is not given, it
       will end up as 0, even though this isn't technically a valid choice.
       This allows detection of when no choice is given.)
     */
    var blop = op.option[Int]("blop", default = 1, choices = Seq(1, 2, 4, 7))
    /* A string option --daniel, with only the values "mene", "tekel", and
       "upharsin" allowed, but where values can be repeated, e.g.
       --daniel mene --daniel mene --daniel tekel --daniel upharsin
       . */
    var daniel = op.multiOption[String]("daniel",
      choices = Seq("mene", "tekel", "upharsin"))
    /* A required positional parameter. */
    var destfile = op.parameter[String]("DESTFILE", help = "Destination file to store output in")
    /* A multi-positional parameter that sucks up all remaining arguments. */
    var files = op.multiParameter[String]("FILES", help = "Files to process")
  }
  // This first call is necessary, even though it doesn't appear to do
  // anything.  In particular, this ensures that all arguments have been
  // defined on `op` prior to parsing.
  val first_shadow = new MyOpts(op)
  // op.parse(List("--foo", "7"))
  op.parse(args)
  val Opts = new MyOpts(op)
  /* Print out values of all arguments, whether options or positional */
  for ((name, value) <- op.argNameValues)
    println("%30s: %s" format (name, value))
}
