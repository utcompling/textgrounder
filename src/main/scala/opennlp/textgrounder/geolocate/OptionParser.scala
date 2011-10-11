package opennlp.textgrounder.geolocate
import org.clapper.argot._
import util.control.Breaks._
import collection.mutable

object OptParse {
  implicit def convertInt(rawval: String, op: OptionSingle[Int]) = {
    try { rawval.toInt }
    catch {
      case e: NumberFormatException =>
        throw new OptParseConversionException(
          "Cannot convert argument \"" + rawval + "\" to a number.")
    }
  }

  implicit def convertDouble(rawval: String, op: OptionSingle[Double]) = {
    try { rawval.toDouble }
    catch {
      case e: NumberFormatException =>
        throw new OptParseConversionException(
          "Cannot convert argument \"" + rawval + "\" to a number.")
    }
  }

  implicit def convertString(rawval: String, op: OptionSingle[String]) = {
    rawval
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
   * {{{
   * import opennlp.textgrounder.geolocate.OptParse._
   *
   * object TestOpts extends App {
   *   val op = new OptionParser("test")
   *   object Opts {
   *     def foo = op.option[Int]("foo", default=5)
   *     def bar = op.option[String]("bar", default="fuck")
   *     def baz = op.multiOption[String]("baz")
   *     def bat = op.multiOption[Int]("bat")
   *     def blop = op.option[String]("blop", choices=Seq("mene", "tekel", "upharsin"))
   *     def blop2 = op.multiOption[String]("blop2", choices=Seq("mene", "tekel", "upharsin"))
   *   }
   *   ...
   *   op.parse(Opts, args)
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
   * Thrown to indicate that OptParse could not convert a command line parameter
   * to the desired type.
   *
   * @param message  exception message
   */
  class OptParseConversionException(message: String)
    extends OptParseException(message)

  /**
   * Thrown to indicate that OptParse encountered a problem in the caller's
   * argument specification. This exception can be interpreted as a bug in
   * the caller's program.
   *
   * @param message  exception message
   */
  class OptParseSpecificationError(message: String)
    extends OptParseException("(BUG) " + message)

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
        throw new OptParseSpecificationError("Need to specify at least one command-line option")
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

  abstract class OptionAny[T] {
    def value: T
    def apply() = value
    def specified: Boolean
  }

  class OptionFlag extends OptionAny[Boolean] {
    var wrap: FlagOption[Boolean] = null
    def value = {
      wrap.value match {
        case Some(x) => x
        case None => false
      }
    }
    def specified = (wrap != null && wrap.value != None)
  }

  class OptionSingle[T](default: T) extends OptionAny[T] {
    var wrap: SingleValueOption[T] = null
    def value = {
      wrap.value match {
        case Some(x) => x
        case None => default
      }
    }
    def specified = (wrap != null && wrap.value != None)
  }

  class OptionMulti[T] extends OptionAny[Seq[T]] {
    var wrap: MultiValueOption[T] = null
    val wrapSingle = new OptionSingle[T](null.asInstanceOf[T])
    def value = wrap.value
    def specified = (wrap != null && wrap.value.length > 0)
  }

  class OptionParser(prog: String) {
    import OptionParser._
    val op = new ArgotParser(prog)
    val argmap = mutable.Map[String, OptionAny[_]]()
    var argholder = null: AnyRef

    def optionSeq[T](opt: Seq[String],
      default: T = null.asInstanceOf[T],
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")(implicit convert: (String, OptionSingle[T]) => T) = {
      val control = controllingOpt(opt)
      if (argmap contains control)
        argmap(control).asInstanceOf[OptionAny[T]].value
      else {
        val met2 = computeMetavar(metavar, opt)
        val option = new OptionSingle(default)
        option.wrap =
          op.option[T](opt.toList, met2, help) {
            (rawval: String, op: SingleValueOption[T]) =>
              {
                val converted = convert(rawval, option)
                checkChoices(converted, choices, canonicalize)
              }
          }
        argmap(control) = option
        null.asInstanceOf[T]
      }
    }

    def option[T](opt1: String, opt2: String = null, opt3: String = null,
      opt4: String = null, opt5: String = null, opt6: String = null,
      opt7: String = null, opt8: String = null, opt9: String = null,
      default: T = null.asInstanceOf[T],
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")(implicit convert: (String, OptionSingle[T]) => T) = {
      optionSeq[T](nonNullVals(opt1, opt2, opt3, opt4, opt5, opt6,
        opt7, opt8, opt9),
        metavar = metavar, default = default, choices = choices,
        canonicalize = canonicalize, help = help)(convert)
    }

    def flagSeq(opt: Seq[String],
      help: String = "") = {
      import ArgotConverters._
      val control = controllingOpt(opt)
      if (argmap contains control)
        argmap(control).asInstanceOf[OptionFlag].value
      else {
        val option = new OptionFlag()
        option.wrap = op.flag[Boolean](opt.toList, help)
        argmap(control) = option
        null.asInstanceOf[Boolean]
      }
    }

    def flag(opt1: String, opt2: String = null, opt3: String = null,
      opt4: String = null, opt5: String = null, opt6: String = null,
      opt7: String = null, opt8: String = null, opt9: String = null,
      help: String = "") = {
      flagSeq(nonNullVals(opt1, opt2, opt3, opt4, opt5, opt6,
        opt7, opt8, opt9),
        help = help)
    }

    def multiOptionSeq[T](opt: Seq[String],
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")(implicit convert: (String, OptionSingle[T]) => T) = {
      val control = controllingOpt(opt)
      if (argmap contains control)
        argmap(control).asInstanceOf[OptionAny[Seq[T]]].value
      else {
        val met2 = computeMetavar(metavar, opt)
        val option = new OptionMulti[T]()
        option.wrap =
          op.multiOption[T](opt.toList, met2, help) {
            (rawval: String, op: MultiValueOption[T]) =>
              {
                val converted = convert(rawval, option.wrapSingle)
                checkChoices(converted, choices, canonicalize)
              }
          }
        argmap(control) = option
        null.asInstanceOf[Seq[T]]
      }
    }

    /**
     * Specify an option that can be repeated multiple times.  The resulting
     * option value will be a List of all the values given on the command line
     * (one value per occurrence of the option).  If there are no occurrences
     * of the option, the value will be an empty List.
     */
    def multiOption[T](opt1: String, opt2: String = null, opt3: String = null,
      opt4: String = null, opt5: String = null, opt6: String = null,
      opt7: String = null, opt8: String = null, opt9: String = null,
      metavar: String = null,
      choices: Seq[T] = null,
      canonicalize: Map[T, Iterable[T]] = null,
      help: String = "")(implicit convert: (String, OptionSingle[T]) => T) = {
      multiOptionSeq[T](nonNullVals(opt1, opt2, opt3, opt4, opt5, opt6,
        opt7, opt8, opt9),
        metavar = metavar, choices = choices,
        canonicalize = canonicalize, help = help)(convert)
    }

    def parse(obj: AnyRef, args: Seq[String]) {
      argholder = obj
      val objargs = obj.getClass.getDeclaredMethods
      // Call each null-argument function, assumed to be an option.  The
      // first time such functions are called, they will install the
      // appropriate OptionAny-subclass object in 
      for {
        arg <- objargs
        if {
          // println(arg)
          // println(arg.getName)
          // println(arg.getParameterTypes)
          // println(arg.getParameterTypes.length)
          // val rettype = arg.getReturnType
          // println(rettype)
          // println(rettype.getName)
          // val rettype2 = arg.getGenericReturnType
          // println(rettype2)
          // println(rettype2.getName)
          (arg.getParameterTypes.length == 0)
          // && arg.getReturnType.isAssignableFrom(classOf[OptionAny[_]]))
        }
      }
      arg.invoke(obj)
      // println(argmap)
      op.parse(args.toList)
    }

    def error(msg: String) {
      throw new OptParseConversionException(msg)
    }

    def check_args_available() {
      assert(argholder != null, "Need to call 'parse()' first so that the arguments are known (they are fetched from the first argument to 'parse()')")
    }

    def get_argmap = {
      check_args_available()
      argmap
    }

    def need(arg: String, arg_english: String = null) {
      val marg_english =
        if (arg_english == null)
          arg.replace("-", " ")
        else
          arg_english
      check_args_available()
      val option = argmap(arg)
      if (!option.specified)
        error("Must specify %s using --%s" format
          (marg_english, arg.replace("_", "-")))
    }
  }
}

object TestOpts extends App {
  import OptParse._
  val op = new OptionParser("test")
  object Opts {
    def foo = op.option[Int]("foo", default = 5)
    def bar = op.option[String]("bar", default = "fuck")
    def baz = op.multiOption[String]("baz")
    def bat = op.multiOption[Int]("bat")
    def blop = op.option[String]("blop", choices = Seq("mene", "tekel", "upharsin"))
    def blop2 = op.multiOption[String]("blop2", choices = Seq("mene", "tekel", "upharsin"))
  }
  // op.parse(Opts, List("--foo", "7"))
  op.parse(Opts, args)
  println("foo: %s" format Opts.foo)
  println("bar: %s" format Opts.bar)
  println("baz: %s" format Opts.baz)
  println("bat: %s" format Opts.bat)
  println("blop: %s" format Opts.blop)
  println("blop2: %s" format Opts.blop2)
}
