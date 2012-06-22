///////////////////////////////////////////////////////////////////////////////
//  argparser.scala
//
//  Copyright (C) 2011, 2012 Ben Wing, The University of Texas at Austin
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

import org.clapper.argot._

/**
  This module implements an argument parser for Scala, which handles
  both options (e.g. --output-file foo.txt) and positional arguments.
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
  (6) The help text can be split across multiple lines using multi-line
      strings, and extra whitespace/carriage returns will be absorbed
      appropriately.  In addition, directives can be given, such as %default
      for the default value, %choices for the list of choices, %metavar
      for the meta-variable (see below), %prog for the name of the program,
      %% for a literal percent sign, etc.
  (7) A reasonable default value is provided for the "meta-variable"
      parameter for options (which specifies the type of the argument).
  (8) Conversion functions are easier to specify, since one function suffices
      for all types of arguments.
  (9) A conversion function is provided for type Boolean for handling
      valueful boolean options, where the value can be any of "yes, no,
      y, n, true, false, t, f, on, off".
  (10) There is no need to manually catch exceptions (e.g. from usage errors)
       if you don't want to.  By default, exceptions are caught
       automatically and printed out nicely, and then the program exits
       with return code 1.  You can turn this off if you want to handle
       exceptions yourself.
  
  In general, to parse arguments, you first create an object of type
  ArgParser (call it `ap`), and then add options to it by calling
  functions, typically:

  -- ap.option[T]() for a single-valued option of type T
  -- ap.multiOption[T]() for a multi-valued option of type T (i.e. the option
       can be specified multiple times on the command line, and all such
       values will be accumulated into a List)
  -- ap.flag() for a boolean flag
  -- ap.positional[T]() for a positional argument (coming after all options)
  -- ap.multiPositional[T]() for a multi-valued positional argument (i.e.
     eating up any remaining positional argument given)

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

  2) A class, e.g. ProgParams, is created to hold the values returned from
     the command line.  This class typically looks like this:
     
     class ProgParams(ap: ArgParser) {
       var outfile = ap.option[String]("outfile", "o", ...)
       var verbose = ap.flag("verbose", "v", ...)
       ...
     }

  3) To parse a command line, we proceed as follows:

     a) Create an ArgParser object.
     b) Create an instance of ProgParams, passing in the ArgParser object.
     c) Call `parse()` on the ArgParser object, to parse a command line.
     d) Create *another* instance of ProgParams, passing in the *same*
        ArgParser object.
     e) Now, the argument values specified on the command line can be
        retrieved from this new instance of ProgParams simply using field
        accesses, and new values can likewise be set using field accesses.

  Note how this actually works.  When the first instance of ProgParams is
  created, the initialization of the variables causes the arguments to be
  specified on the ArgParser -- and the variables have the default values
  of the arguments.  When the second instance is created, after parsing, and
  given the *same* ArgParser object, the respective calls to "initialize"
  the arguments have no effect, but now return the values specified on the
  command line.  Because these are declared as `var`, they can be freely
  re-assigned.  Furthermore, because no actual reflection or any such thing
  is done, the above scheme will work completely fine if e.g. ProgParams
  subclasses another class that also declares some arguments (e.g. to
  abstract out common arguments for multiple applications).  In addition,
  there is no problem mixing and matching the scheme described here with the
  conceptually simpler scheme where argument values are retrieved using
  `ap.get[T]()`.

  Work being considered:

  (1) Perhaps most important: Allow grouping of options.  Groups would be
      kept together in the usage message, displayed under the group name.
  (2) Add constructor parameters to allow for specification of the other
      things allowed in Argot, e.g. pre-usage, post-usage, whether to sort
      arguments in the usage message or leave as-is.
  (3) Provide an option to control how meta-variable generation works.
      Normally, an unspecified meta-variable is derived from the
      canonical argument name in all uppercase (e.g. FOO or STRATEGY),
      but some people e.g. might instead want it derived from the argument
      type (e.g. NUM or STRING).  This would be controlled with a
      constructor parameter to ArgParser.
  (4) Add converters for other basic types, e.g. Float, Char, Byte, Short.
  (5) Allow for something similar to Argot's typed flags (and related
      generalizations).  I'd call it `ap.typedFlag[T]()` or something
      similar.  But rather than use Argot's interface of "on" and "off"
      flags, I'd prefer to follow the lead of Python's argparse, allowing
      the "destination" argument name to be specified independently of
      the argument name as it appears on the command line, so that
      multiple arguments could write to the same place.  I'd also add a
      "const" parameter that stores an arbitrary constant value if a
      flag is tripped, so that you could simulate the equivalent of a
      limited-choice option using multiple flag options.  In addition,
      I'd add an optional "action" parameter that is passed in the old
      and new values and returns the actual value to be stored; that
      way, incrementing/decrementing or whatever could be implemented.
      Note that I believe it's better to separate the conversion and
      action routines, unlike what Argot does -- that way, the action
      routine can work with properly-typed values and doesn't have to
      worry about how to convert them to/from strings.  This also makes
      it possible to supply action routines for all the various categories
      of arguments (e.g. flags, options, multi-options), while keeping
      the conversion routines simple -- the action routines necessarily
      need to be differently-typed at least for single vs.  multi-options,
      but the conversion routines shouldn't have to worry about this.
      In fact, to truly implement all the generality of Python's 'argparse'
      routine, we'd want expanded versions of option[], multiOption[],
      etc. that take both a source type (to which the raw values are
      initially converted) and a destination type (final type of the
      value stored), so that e.g. a multiOption can sum values into a
      single 'accumulator' destination argument, or a single String
      option can parse a classpath into a List of File objects, or
      whatever. (In fact, however, I think it's better to dispense
      with such complexity in the ArgParser and require instead that the
      calling routine deal with it on its own.  E.g. there's absolutely
      nothing preventing a calling routine using field-style argument
      values from declaring extra vars to hold destination values and
      then e.g. simply fetching the classpath value, parsing it and
      storing it, or fetching all values of a multiOption and summing
      them.  The minimal support for the Argot example of increment and
      decrement flags would be something like a call `ap.multiFlag`
      that accumulates a list of Boolean "true" values, one per
      invocation.  Then we just count the number of increment flags and
      number of decrement flags given.  If we cared about the relative
      way that these two flags were interleaved, we'd need a bit more
      support -- (1) a 'destination' argument to allow two options to
      store into the same place; (2) a typed `ap.multiFlag[T]`; (3)
      a 'const' argument to specify what value to store.  Then our
      destination gets a list of exactly which flags were invoked and
      in what order. On the other hand, it's easily arguable that no
      program should have such intricate option processing that requires
      this -- it's unlikely the user will have a good understanding
      of what these interleaved flags end up doing.
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
          Params.files
        else
          Seq[String]()

      This unifies to AnyRef, not Seq[String], even if Params.files wraps
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

  /**
   * Implicit conversion function for Ints.  Automatically selected
   * for Int-type arguments.
   */
  implicit def convertInt(rawval: String, name: String, ap: ArgParser) = {
    try { rawval.toInt }
    catch {
      case e: NumberFormatException =>
        throw new ArgParserConversionException(
          """Cannot convert argument "%s" to an integer.""" format rawval)
    }
  }

  /**
   * Implicit conversion function for Doubles.  Automatically selected
   * for Int-type arguments.
   */
  implicit def convertDouble(rawval: String, name: String, ap: ArgParser) = {
    try { rawval.toDouble }
    catch {
      case e: NumberFormatException =>
        throw new ArgParserConversionException(
          """Cannot convert argument "%s" to a floating-point number."""
          format rawval)
    }
  }

  /**
   * Check restrictions on `value`, the parsed value for option named
   * `name`.  Restrictions can specify that the number must be greater than or
   * equal, or strictly greater than, a given number -- and/or that the
   * number must be less than or equal, or strictly less than, a given
   * number.  Signal an error if restrictions not met.
   *
   * @tparam T Numeric type of `value` (e.g. Int or Double).
   * @param minposs Required: Minimum possible value for this numeric type;
   *   used as the default value for certain non-specified arguments.
   * @param maxposs Required: Maximum possible value for this numeric type;
   *   used as the default value for certain non-specified arguments.
   * @param gt If specified, `value` must be greater than the given number.
   * @param ge If specified, `value` must be greater than or equal to the
   *   given number.
   * @param lt If specified, `value` must be less than the given number.
   * @param le If specified, `value` must be less than or equal to the
   *   given number.
   */
  def check_restrictions[T <% Ordered[T]](value: T, name: String, ap: ArgParser,
      minposs: T, maxposs: T)(gt: T = minposs, ge: T = minposs,
      lt: T = maxposs, le: T = maxposs) {
    val has_lower_bound = !(gt == minposs && ge == minposs)
    val has_upper_bound = !(lt == maxposs && le == maxposs)
    val has_open_lower_bound = (gt != minposs)
    val has_open_upper_bound = (lt != maxposs)

    def check_lower_bound(): String = {
      if (!has_lower_bound) null
      else if (has_open_lower_bound) {
        if (value > gt) null
        else "strictly greater than %s" format gt
      } else {
        if (value >= ge) null
        else "at least %s" format ge
      }
    }
    def check_upper_bound(): String = {
      if (!has_upper_bound) null
      else if (has_open_upper_bound) {
        if (value < lt) null
        else "strictly less than %s" format lt
      } else {
        if (value <= le) null
        else "at most %s" format le
      }
    }

    val lowerstr = check_lower_bound()
    val upperstr = check_upper_bound()

    def range_error(restriction: String) {
      val msg = """Argument "%s" has value %s, but must be %s.""" format
        (name, value, restriction)
      throw new ArgParserRangeException(msg)
    }

    if (lowerstr != null && upperstr != null)
      range_error("%s and %s" format (lowerstr, upperstr))
    else if (lowerstr != null)
      range_error("%s" format lowerstr)
    else if (upperstr != null)
      range_error("%s" format upperstr)
  }

  /**
   * Conversion function for Ints.  Also check that the result meets the
   * given restrictions (conditions).
   */
  def convertRestrictedInt(
    gt: Int = Int.MinValue, ge: Int = Int.MinValue,
    lt: Int = Int.MaxValue, le: Int = Int.MaxValue
  ) = {
    (rawval: String, name: String, ap: ArgParser) => {
      val retval = convertInt(rawval, name, ap)
      check_restrictions[Int](retval, name, ap, Int.MinValue, Int.MaxValue)(
        gt = gt, ge = ge, lt = lt, le = le)
      retval
    }
  }

  /**
   * Conversion function for Doubles.  Also check that the result meets the
   * given restrictions (conditions).
   */
  def convertRestrictedDouble(
    gt: Double = Double.NegativeInfinity, ge: Double = Double.NegativeInfinity,
    lt: Double = Double.PositiveInfinity, le: Double = Double.PositiveInfinity
  ) = {
    (rawval: String, name: String, ap: ArgParser) => {
      val retval = convertDouble(rawval, name, ap)
      check_restrictions[Double](retval, name, ap,
        Double.NegativeInfinity, Double.PositiveInfinity)(
        gt = gt, ge = ge, lt = lt, le = le)
      retval
    }
  }

  /**
   * Conversion function for positive Int.  Checks that the result is &gt; 0.
   */
  def convertPositiveInt = convertRestrictedInt(gt = 0)
  /**
   * Conversion function for non-negative Int.  Check that the result is &gt;=
   * 0.
   */
  def convertNonNegativeInt = convertRestrictedInt(ge = 0)

  /**
   * Conversion function for positive Double.  Checks that the result is &gt; 0.
   */
  def convertPositiveDouble = convertRestrictedDouble(gt = 0)
  /**
   * Conversion function for non-negative Double.  Check that the result is &gt;=
   * 0.
   */
  def convertNonNegativeDouble = convertRestrictedDouble(ge = 0)

  /**
   * Implicit conversion function for Strings.  Automatically selected
   * for String-type arguments.
   */
  implicit def convertString(rawval: String, name: String, ap: ArgParser) = {
    rawval
  }

  /**
   * Implicit conversion function for Boolean arguments, used for options
   * that take a value (rather than flags).
   */
  implicit def convertBoolean(rawval: String, name: String, ap: ArgParser) = {
    rawval.toLowerCase match {
      case "yes" => true
      case "no" => false
      case "y" => true
      case "n" => false
      case "true" => true
      case "false" => false
      case "t" => true
      case "f" => false
      case "on" => true
      case "off" => false
      case _ => throw new ArgParserConversionException(
          ("""Cannot convert argument "%s" to a boolean.  """ +
           """Recognized values (case-insensitive) are """ +
           """yes, no, y, n, true, false, t, f, on, off.""") format rawval)
    }
  }

  /**
   * Superclass of all exceptions related to `argparser`.  These exceptions
   * are generally thrown during argument parsing.  Normally, the exceptions
   * are automatically caught, their message displayed, and then the
   * program exited with code 1, indicating a problem.  However, this
   * behavior can be suppressed by setting the constructor parameter
   * `catchErrors` on `ArgParser` to false.  In such a case, the exceptions
   * will be propagated to the caller, which should catch them and handle
   * appropriately; otherwise, the program will be terminated with a stack
   * trace.
   *
   * @param message Message of the exception
   * @param cause If not None, an exception, used for exception chaining
   *   (when one exception is caught, wrapped in another exception and
   *   rethrown)
   */
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
   * Thrown to indicate usage errors.
   *
   * @param message fully fleshed-out usage string.
   * @param cause exception, if propagating an exception
   */
  class ArgParserUsageException(
    message: String,
    cause: Option[Throwable] = None
  ) extends ArgParserException(message, cause)

  /**
   * Thrown to indicate that ArgParser could not convert a command line
   * argument to the desired type.
   *
   * @param message exception message
   * @param cause exception, if propagating an exception
   */
  class ArgParserConversionException(
    message: String,
    cause: Option[Throwable] = None
  ) extends ArgParserException(message, cause)

  /**
   * Thrown to indicate that a command line argument was outside of a
   * required range.
   *
   * @param message exception message
   * @param cause exception, if propagating an exception
   */
  class ArgParserRangeException(
    message: String,
    cause: Option[Throwable] = None
  ) extends ArgParserException(message, cause)

  /**
   * Thrown to indicate that an invalid choice was given for a limited-choice
   * argument.  The message indicates both the problem and the list of
   * possible choices.
   *
   * @param message  exception message
   */
  class ArgParserInvalidChoiceException(message: String,
    cause: Option[Throwable] = None
  ) extends ArgParserConversionException(message, cause)

  /**
   * Thrown to indicate that ArgParser encountered a problem in the caller's
   * argument specification, or something else indicating invalid coding.
   * This indicates a bug in the caller's code.  These exceptions are not
   * automatically caught.
   *
   * @param message  exception message
   */
  class ArgParserCodingError(message: String,
    cause: Option[Throwable] = None
  ) extends ArgParserException("(CALLER BUG) " + message, cause)

  /**
   * Thrown to indicate that ArgParser encountered a problem that should
   * never occur under any circumstances, indicating a bug in the ArgParser
   * code itself.  These exceptions are not automatically caught.
   *
   * @param message  exception message
   */
  class ArgParserInternalError(message: String,
    cause: Option[Throwable] = None
  ) extends ArgParserException("(INTERNAL BUG) " + message, cause)

  /* Some static functions related to ArgParser; all are for internal use */
  protected object ArgParser {
    // Given a list of aliases for an argument, return the canonical one
    // (first one that's more than a single letter).
    def canonName(name: Seq[String]): String = {
      assert(name.length > 0)
      for (n <- name) {
        if (n.length > 1) return n
      }
      return name(0)
    }

    // Compute the metavar for an argument.  If the metavar has already
    // been given, use it; else, use the upper case version of the
    // canonical name of the argument.
    def computeMetavar(metavar: String, name: Seq[String]) = {
      if (metavar != null) metavar
      else canonName(name).toUpperCase
    }

    // Return a sequence of all the given strings that aren't null.
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

    // Convert aliases map to a map in the other direction, i.e. from
    // alias to canonical choice
    def reverseAliases[T](aliases: Map[T, Iterable[T]]) = {
      if (aliases == null)
        Map[T, T]()
      else
        for {(full, abbrevs) <- aliases
             abbrev <- if (abbrevs == null) Seq() else abbrevs}
          yield (abbrev -> full)
    }

    // Get rid of nulls in `choices` or `aliases`.  If `aliases` is null,
    // make it an empty map.  If `choices` is null, derive it from the
    // canonical choices given in the `aliases` map.  Note that before
    // calling this, a special check must be done for the case where
    // `choices` is null *and* `aliases` is null, which is actually the
    // most common situation.  In this case, no limited-choice restrictions
    // apply at all.  Returns a tuple of canonicalized choices,
    // canonicalized aliases.
    def canonicalizeChoicesAliases[T](choices: Seq[T],
        aliases: Map[T, Iterable[T]]) = {
      val newaliases = if (aliases != null) aliases else Map[T, Iterable[T]]()
      val newchoices =
        if (choices != null) choices else newaliases.keys.toSeq
      (newchoices, newaliases)
    }

    // Return a human-readable list of all choices.  If 'choices' is null,
    // the list is derived from the canonical choices listed in 'aliases'.
    // If 'include_aliases' is true, include the aliases in the list of
    // choices, in parens after the canonical name.
    def choicesList[T](choices: Seq[T],
        aliases: Map[T, Iterable[T]], include_aliases: Boolean) = {
      val (newchoices, newaliases) =
        canonicalizeChoicesAliases(choices, aliases)
      val sorted_choices = newchoices sortBy (_.toString)
      if (!include_aliases)
        sorted_choices mkString ", "
      else {
        (
          for { choice <- sorted_choices
                choice_aliases_raw = newaliases.getOrElse(choice, null)
                choice_aliases = (
                  if (choice_aliases_raw != null) choice_aliases_raw
                  else Seq[T]()).toSeq sortBy (_.toString)
              }
          yield {
            if (choice_aliases.length > 0)
              "%s (%s)" format (choice, choice_aliases mkString "/")
            else choice
          }
        ) mkString ", "
      }
    }

    // Check that the given value passes any restrictions imposed by
    // `choices` and/or `aliases`.  If not, throw an exception.
    def checkChoices[T](converted: T, choices: Seq[T],
        aliases: Map[T, Iterable[T]]) = {
      if (choices == null && aliases == null) converted
      else {
        var retval = converted
        val (newchoices, newaliases) =
          canonicalizeChoicesAliases(choices, aliases)
        val revaliases = reverseAliases(aliases)
        retval = revaliases.getOrElse(retval, retval)
        if (newchoices contains retval) retval
        else
          throw new ArgParserInvalidChoiceException("Choice '%s' not one of the recognized choices: %s" format (retval, choicesList(choices, aliases, true)))
      }
    }
  }

  /**
   * Base class of all argument-wrapping classes.  These are used to
   * wrap the appropriate argument-category class from Argot, and return
   * values by querying Argot for the value, returning the default value
   * if Argot doesn't have a value recorded.
   *
   * NOTE that these classes are not meant to leak out to the user.  They
   * should be considered implementation detail only and subject to change.
   *
   * @param parser ArgParser for which this argument exists.
   * @param name Name of the argument.
   * @param default Default value of the argument, used when the argument
   *   wasn't specified on the command line.
   * @tparam T Type of the argument (e.g. Int, Double, String, Boolean).
   */

  abstract protected class ArgAny[T](
    val parser: ArgParser,
    val name: String,
    val default: T
  ) {
    /**
     * Return the value of the argument, if specified; else, the default
     * value. */
    def value = {
      if (overridden)
        overriddenValue
      else if (specified)
        wrappedValue
      else
        default
    }

    def setValue(newval: T) {
      overriddenValue = newval
      overridden = true
    }

    /**
     * When dereferenced as a function, also return the value.
     */
    def apply() = value

    /**
     * Whether the argument's value was specified.  If not, the default
     * value applies.
     */
    def specified: Boolean

    /**
     * Clear out any stored values so that future queries return the default.
     */
    def clear() {
      clearWrapped()
      overridden = false
    }

    /**
     * Return the value of the underlying Argot object, assuming it exists
     * (possibly error thrown if not).
     */
    protected def wrappedValue: T

    /**
     * Clear away the wrapped value.
     */
    protected def clearWrapped()

    /**
     * Value if the user explicitly set a value.
     */
    protected var overriddenValue: T = _

    /**
     * Whether the user explicit set a value.
     */
    protected var overridden: Boolean = false

  }

  /**
   * Class for wrapping simple Boolean flags.
   *
   * @param parser ArgParser for which this argument exists.
   * @param name Name of the argument.
   */

  protected class ArgFlag(
    parser: ArgParser,
    name: String
  ) extends ArgAny[Boolean](parser, name, false) {
    var wrap: FlagOption[Boolean] = null
    def wrappedValue = wrap.value.get
    def specified = (wrap != null && wrap.value != None)
    def clearWrapped() { if (wrap != null) wrap.reset() }
  }

  /**
   * Class for wrapping a single (non-multi) argument (either option or
   * positional param).
   *
   * @param parser ArgParser for which this argument exists.
   * @param name Name of the argument.
   * @param default Default value of the argument, used when the argument
   *   wasn't specified on the command line.
   * @param is_positional Whether this is a positional argument rather than
   *   option (default false).
   * @tparam T Type of the argument (e.g. Int, Double, String, Boolean).
   */

  protected class ArgSingle[T](
    parser: ArgParser,
    name: String,
    default: T,
    val is_positional: Boolean = false
  ) extends ArgAny[T](parser, name, default) {
    var wrap: SingleValueArg[T] = null
    def wrappedValue = wrap.value.get
    def specified = (wrap != null && wrap.value != None)
    def clearWrapped() { if (wrap != null) wrap.reset() }
  }

  /**
   * Class for wrapping a multi argument (either option or positional param).
   *
   * @param parser ArgParser for which this argument exists.
   * @param name Name of the argument.
   * @param default Default value of the argument, used when the argument
   *   wasn't specified on the command line even once.
   * @param is_positional Whether this is a positional argument rather than
   *   option (default false).
   * @tparam T Type of the argument (e.g. Int, Double, String, Boolean).
   */

  protected class ArgMulti[T](
    parser: ArgParser,
    name: String,
    default: Seq[T],
    val is_positional: Boolean = false
  ) extends ArgAny[Seq[T]](parser, name, default) {
    var wrap: MultiValueArg[T] = null
    val wrapSingle = new ArgSingle[T](parser, name, null.asInstanceOf[T])
    def wrappedValue = wrap.value
    def specified = (wrap != null && wrap.value.length > 0)
    def clearWrapped() { if (wrap != null) wrap.reset() }
  }

  /**
   * Main class for parsing arguments from a command line.
   *
   * @param prog Name of program being run, for the usage mssage.
   * @param description Text describing the operation of the program.  It is
   *   placed between the line "Usage: ..." and the text describing the
   *   options and positional arguments; hence, it should not include either
   *   of these, just a description.
   * @param preUsage Optional text placed before the usage message (e.g.
   *   a copyright and/or version string).
   * @param postUsage Optional text placed after the usage message.
   * @param return_defaults If true, field values in field-based value
   *  access always return the default value, even aft3r parsing.
   */
  class ArgParser(prog: String,
      description: String = "",
      preUsage: String = "",
      postUsage: String = "",
      return_defaults: Boolean = false) {
    import ArgParser._
    import ArgotConverters._
    /* The underlying ArgotParser object. */
    protected val argot = new ArgotParser(prog,
      description = if (description.length > 0) Some(description) else None,
      preUsage = if (preUsage.length > 0) Some(preUsage) else None,
      postUsage = if (postUsage.length > 0) Some(postUsage) else None)
    /* A map from the argument's canonical name to the subclass of ArgAny
       describing the argument and holding its value.  The canonical name
       of options comes from the first non-single-letter name.  The
       canonical name of positional arguments is simply the name of the
       argument.  Iteration over the map yields keys in the order they
       were added rather than random. */
    protected val argmap = mutable.LinkedHashMap[String, ArgAny[_]]()
    /* The type of each argument.  For multi options and multi positional
       arguments this will be of type Seq.  Because of type erasure, the
       type of sequence must be stored separately, using argtype_multi. */
    protected val argtype = mutable.Map[String, Class[_]]()
    /* For multi arguments, the type of each individual argument. */
    protected val argtype_multi = mutable.Map[String, Class[_]]()
    /* Set specifying arguments that are positional arguments. */
    protected val argpositional = mutable.Set[String]()
    /* Set specifying arguments that are flag options. */
    protected val argflag = mutable.Set[String]()

    /* NOTE NOTE NOTE: Currently we don't provide any programmatic way of
       accessing the ArgAny-subclass object by name.  This is probably
       a good thing -- these objects can be viewed as internal
    */
    /**
     * Return the value of an argument, or the default if not specified.
     *
     * @param arg The canonical name of the argument, i.e. the first
     *   non-single-letter alias given.
     * @return The value, of type Any.  It must be cast to the appropriate
     *   type.
     * @see #get[T]
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
     * @return The value, of type T.
     */
    def get[T](arg: String) = argmap(arg).asInstanceOf[ArgAny[T]].value

    /**
     * Explicitly set the value of an argument.
     *
     * @param arg The canonical name of the argument, i.e. the first
     *   non-single-letter alias given.
     * @param value The new value of the argument.
     * @tparam T The type of the argument, which must match the type given
     *   in its definition
     *   
     * @return The value, of type T.
     */
    def set[T](arg: String, value: T) {
      argmap(arg).asInstanceOf[ArgAny[T]].setValue(value)
    }

    /**
     * Return the default value of an argument.
     *
     * @param arg The canonical name of the argument, i.e. the first
     *   non-single-letter alias given.
     * @tparam T The type of the argument, which must match the type given
     *   in its definition
     *   
     * @return The value, of type T.
     */
    def defaultValue[T](arg: String) =
      argmap(arg).asInstanceOf[ArgAny[T]].default

    /**
     * Return whether an argument (either option or positional argument)
     * exists with the given canonical name.
     */
    def exists(arg: String) = argmap contains arg

    /**
     * Return whether an argument exists with the given canonical name.
     */
    def isOption(arg: String) = exists(arg) && !isPositional(arg)

    /**
     * Return whether a positional argument exists with the given name.
     */
    def isPositional(arg: String) = argpositional contains arg

    /**
     * Return whether a flag option exists with the given canonical name.
     */
    def isFlag(arg: String) = argflag contains arg

    /**
     * Return whether a multi argument (either option or positional argument)
     * exists with the given canonical name.
     */
    def isMulti(arg: String) = argtype_multi contains arg

    /**
     * Return whether the given argument's value was specified.  If not,
     * fetching the argument's value returns its default value instead.
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

    /**
     * Iterate over all defined arguments.
     *
     * @return an Iterable over the names of the arguments.  The argument
     *   categories (e.g. option, multi-option, flag, etc.), argument
     *   types (e.g. Int, Boolean, Double, String, Seq[String]), default
     *   values and actual values can be retrieved using other functions.
     */
    def argNames: Iterable[String] = {
      for ((name, argobj) <- argmap) yield name
    }

    protected def handle_argument[T : Manifest, U : Manifest](
      name: Seq[String],
      default: U,
      metavar: String,
      choices: Seq[T] = null,
      aliases: Map[T, Iterable[T]] = null,
      help: String,
      create_underlying: (String, String, String) => ArgAny[U],
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
        val canon_metavar = computeMetavar(metavar, name)
        val helpsplit = """(%%|%default|%choices|%allchoices|%metavar|%prog|%|[^%]+)""".r.findAllIn(
          help.replaceAll("""\s+""", " "))
        val canon_help =
          (for (s <- helpsplit) yield {
            s match {
              case "%default" => default.toString
              case "%choices" => choicesList(choices, aliases, false)
              case "%allchoices" => choicesList(choices, aliases, true)
              case "%metavar" => canon_metavar
              case "%%" => "%"
              case "%prog" => this.prog
              case _ => s
            }
          }) mkString ""
        val underobj = create_underlying(canon, canon_metavar, canon_help)
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

    protected def argot_converter[T](convert: (String, String, ArgParser) => T,
        canon_name: String, choices: Seq[T], aliases: Map[T, Iterable[T]]) = {
      (rawval: String, argop: CommandLineArgument[T]) => {
        val converted = convert(rawval, canon_name, this)
        checkChoices(converted, choices, aliases)
      }
    }

    def optionSeq[T](name: Seq[String],
      default: T = null.asInstanceOf[T],
      metavar: String = null,
      choices: Seq[T] = null,
      aliases: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, String, ArgParser) => T, m: Manifest[T]) = {
      def create_underlying(canon_name: String, canon_metavar: String,
          canon_help: String) = {
        val arg = new ArgSingle(this, canon_name, default)
        arg.wrap =
          (argot.option[T](name.toList, canon_metavar, canon_help)
           (argot_converter(convert, canon_name, choices, aliases)))
        arg
      }
      handle_argument[T,T](name, default, metavar, choices, aliases,
        help, create_underlying _)
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
     *    allowed. (But see the `aliases` param below.) Otherwise, all
     *    values will be allowed.
     * @param aliases Mapping specifying aliases for choice values.
     *    The idea is that when there are a limited number of choices, there
     *    may be multiple aliases for a given choice, e.g. "dev", "devel" or
     *    "development".  If given, this should be a map, with the key a
     *    canonical choice and the value a sequence of aliases.  Note that
     *    if `aliases` is given but `choices` is not given, the list
     *    of possible choices is automatically derived from the keys of the
     *    `aliases` mapping.  To this end, choices with no aliases
     *    should be listed with `null` as the value instead of a sequence.
     * @param help Help string for the option, shown in the usage string.
     * @param convert Function to convert the raw option (a string) into
     *    a value of type `T`.  The second and third parameters specify
     *    the name of the argument whose value is being converted, and the
     *    ArgParser object that the argument is defined on.  Under normal
     *    circumstances, these parameters should not affect the result of
     *    the conversion function.  For standard types, no conversion
     *    function needs to be specified, as the correct conversion function
     *    will be located automatically through Scala's 'implicit' mechanism.
     * @tparam T The type of the option.  For non-standard types, a
     *    converter must explicitly be given. (The standard types recognized
     *    are currently Int, Double, Boolean and String.)
     *
     * @return If class parameter `return_defaults` is true, the default
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
      aliases: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, String, ArgParser) => T, m: Manifest[T]) = {
      optionSeq[T](nonNullVals(name1, name2, name3, name4, name5, name6,
        name7, name8, name9),
        metavar = metavar, default = default, choices = choices,
        aliases = aliases, help = help)(convert, m)
    }

    def flagSeq(name: Seq[String],
      help: String = "") = {
      import ArgotConverters._
      def create_underlying(canon_name: String, canon_metavar: String,
          canon_help: String) = {
        val arg = new ArgFlag(this, canon_name)
        arg.wrap = argot.flag[Boolean](name.toList, canon_help)
        arg
      }
      handle_argument[Boolean,Boolean](name, false, null, Seq(true, false),
        null, help, create_underlying _)
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
      aliases: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, String, ArgParser) => T, m: Manifest[T]) = {
      def create_underlying(canon_name: String, canon_metavar: String,
          canon_help: String) = {
        val arg = new ArgMulti[T](this, canon_name, default)
        arg.wrap =
          (argot.multiOption[T](name.toList, canon_metavar, canon_help)
           (argot_converter(convert, canon_name, choices, aliases)))
        arg
      }
      handle_argument[T,Seq[T]](name, default, metavar, choices, aliases,
        help, create_underlying _, is_multi = true)
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
      aliases: Map[T, Iterable[T]] = null,
      help: String = "")
    (implicit convert: (String, String, ArgParser) => T, m: Manifest[T]) = {
      multiOptionSeq[T](nonNullVals(name1, name2, name3, name4, name5, name6,
        name7, name8, name9),
        default = default, metavar = metavar, choices = choices,
        aliases = aliases, help = help)(convert, m)
    }

    /**
     * Specify a positional argument.  Positional argument are processed
     * in order.  Optional argument must occur after all non-optional
     * argument.  The name of the argument is only used in the usage file
     * and as the "name" parameter of the ArgSingle[T] object passed to
     * the (implicit) conversion routine.  Usually the name should be in
     * all caps.
     *
     * @see #multiPositional[T]
     */
    def positional[T](name: String,
      default: T = null.asInstanceOf[T],
      choices: Seq[T] = null,
      aliases: Map[T, Iterable[T]] = null,
      help: String = "",
      optional: Boolean = false)
    (implicit convert: (String, String, ArgParser) => T, m: Manifest[T]) = {
      def create_underlying(canon_name: String, canon_metavar: String,
          canon_help: String) = {
        val arg = new ArgSingle(this, canon_name, default, is_positional = true)
        arg.wrap =
          (argot.parameter[T](canon_name, canon_help, optional)
           (argot_converter(convert, canon_name, choices, aliases)))
        arg
      }
      handle_argument[T,T](Seq(name), default, null, choices, aliases,
        help, create_underlying _, is_positional = true)
    }

    /**
     * Specify any number of positional arguments.  These must come after
     * all other arguments.
     *
     * @see #positional[T].
     */
    def multiPositional[T](name: String,
      default: Seq[T] = Seq[T](),
      choices: Seq[T] = null,
      aliases: Map[T, Iterable[T]] = null,
      help: String = "",
      optional: Boolean = true)
    (implicit convert: (String, String, ArgParser) => T, m: Manifest[T]) = {
      def create_underlying(canon_name: String, canon_metavar: String,
          canon_help: String) = {
        val arg = new ArgMulti[T](this, canon_name, default, is_positional = true)
        arg.wrap =
          (argot.multiParameter[T](canon_name, canon_help, optional)
           (argot_converter(convert, canon_name, choices, aliases)))
        arg
      }
      handle_argument[T,Seq[T]](Seq(name), default, null, choices,
        aliases, help, create_underlying _,
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
     * @param catchErrors If true (the default), usage errors will
     *   be caught, a message outputted (without a stack trace), and
     *   the program will exit.  Otherwise, the errors will be allowed
     *   through, and the application should catch them.
     */
    def parse(args: Seq[String], catchErrors: Boolean = true) = {
      if (argmap.size == 0)
        throw new ArgParserCodingError("No arguments initialized.  If you thought you specified arguments, you might have defined the corresponding fields with 'def' instead of 'var' or 'val'.")
      
      def call_parse() {
        // println(argmap)
        try {
          argot.parse(args.toList)
        } catch {
          case e: ArgotUsageException => {
            throw new ArgParserUsageException(e.message, Some(e))
          }
        }
      }

      // Reset everything, in case the user explicitly set some values
      // (which otherwise override values retrieved from parsing)
      clear()
      if (catchErrors) {
        try {
          call_parse()
        } catch {
          case e: ArgParserException => {
            System.out.println(e.message)
            System.exit(1)
          }
        }
      } else call_parse()
    }

    /**
     * Clear all arguments back to their default values.
     */
    def clear() {
      for (obj <- argmap.values) {
        obj.clear()
      }
    }

    def error(msg: String) {
      throw new ArgParserConversionException(msg)
    }
  }
}

object TestArgParser extends App {
  import argparser._
  class MyParams(ap: ArgParser) {
    /* An integer option named --foo, with a default value of 5.  Can also
       be specified using --spam or -f. */
    var foo = ap.option[Int]("foo", "spam", "f", default = 5,
    help="""An integer-valued option.  Default %default.""")
    /* A string option named --bar, with a default value of "chinga".  Can
       also be specified using -b. */
    var bar = ap.option[String]("bar", "b", default = "chinga")
    /* A string option named --baz, which can be given multiple times.
       Default value is an empty sequence. */
    var baz = ap.multiOption[String]("baz")
    /* A floating-point option named --tick, which can be given multiple times.
       Default value is the sequence Seq(2.5, 5.0, 9.0), which will obtain
       when the option hasn't been given at all. */
    var tick = ap.multiOption[Double]("tick", default = Seq(2.5, 5.0, 9.0),
    help = """Option --tick, perhaps for specifying the position of
tick marks along the X axis.  Multiple such options can be given.  If
no marks are specified, the default is %default.  Note that we can
         freely insert
         spaces and carriage
         returns into the help text; whitespace is compressed
      to a single space.""")
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
    var blop = ap.option[Int]("blop", default = 1, choices = Seq(1, 2, 4, 7),
    help = """An integral argument with limited choices.  Default is %default,
possible choices are %choices.""")
    /* A string option --daniel, with only the values "mene", "tekel", and
       "upharsin" allowed, but where values can be repeated, e.g.
       --daniel mene --daniel mene --daniel tekel --daniel upharsin
       . */
    var daniel = ap.multiOption[String]("daniel",
      choices = Seq("mene", "tekel", "upharsin"))
    var strategy =
      ap.multiOption[String]("s", "strategy",
        aliases = Map(
          "baseline" -> null, "none" -> null,
          "full-kl-divergence" ->
            Seq("full-kldiv", "full-kl"),
          "partial-kl-divergence" ->
            Seq("partial-kldiv", "partial-kl", "part-kl"),
          "symmetric-full-kl-divergence" ->
            Seq("symmetric-full-kldiv", "symmetric-full-kl", "sym-full-kl"),
          "symmetric-partial-kl-divergence" ->
            Seq("symmetric-partial-kldiv", "symmetric-partial-kl", "sym-part-kl"),
          "cosine-similarity" ->
            Seq("cossim"),
          "partial-cosine-similarity" ->
            Seq("partial-cossim", "part-cossim"),
          "smoothed-cosine-similarity" ->
            Seq("smoothed-cossim"),
          "smoothed-partial-cosine-similarity" ->
            Seq("smoothed-partial-cossim", "smoothed-part-cossim"),
          "average-cell-probability" ->
            Seq("avg-cell-prob", "acp"),
          "naive-bayes-with-baseline" ->
            Seq("nb-base"),
          "naive-bayes-no-baseline" ->
            Seq("nb-nobase")),
        help = """A multi-string option.  This is an actual option in
one of my research programs.  Possible choices are %choices; the full list
of choices, including all aliases, is %allchoices.""")
    /* A required positional argument. */
    var destfile = ap.positional[String]("DESTFILE",
      help = "Destination file to store output in")
    /* A multi-positional argument that sucks up all remaining arguments. */
    var files = ap.multiPositional[String]("FILES", help = "Files to process")
  }
  val ap = new ArgParser("test")
  // This first call is necessary, even though it doesn't appear to do
  // anything.  In particular, this ensures that all arguments have been
  // defined on `ap` prior to parsing.
  new MyParams(ap)
  // ap.parse(List("--foo", "7"))
  ap.parse(args)
  val Params = new MyParams(ap)
  // Print out values of all arguments, whether options or positional.
  // Also print out types and default values.
  for (name <- ap.argNames)
    println("%30s: %s (%s) (default=%s)" format (
      name, ap(name), ap.getType(name), ap.defaultValue[Any](name)))
  // Examples of how to retrieve individual arguments
  for (file <- Params.files)
    println("Process file: %s" format file)
  println("Maximum tick mark seen: %s" format (Params.tick max))
  // We can freely change the value of arguments if we want, since they're
  // just vars.
  if (Params.daniel contains "upharsin")
    Params.bar = "chingamos"
}
