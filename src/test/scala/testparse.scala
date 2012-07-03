import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical._
import scala.util.parsing.input.CharArrayReader.EofCh

sealed abstract class Expr {
  def matches(x: Seq[String]): Boolean
}

case class EConst(value: Seq[String]) extends Expr {
  def matches(x: Seq[String]) = x containsSlice value
}

case class EAnd(left:Expr, right:Expr) extends Expr {
  def matches(x: Seq[String]) = left.matches(x) && right.matches(x)
}

case class EOr(left:Expr, right:Expr) extends Expr {
  def matches(x: Seq[String]) = left.matches(x) || right.matches(x)
}

case class ENot(e:Expr) extends Expr {
  def matches(x: Seq[String]) = !e.matches(x)
}

class ExprLexical extends StdLexical {
  override def token: Parser[Token] = floatingToken | super.token

  def floatingToken: Parser[Token] =
    rep1(digit) ~ optFraction ~ optExponent ^^
      { case intPart ~ frac ~ exp => NumericLit(
          (intPart mkString "") :: frac :: exp :: Nil mkString "")}

  def chr(c:Char) = elem("", ch => ch==c )
  def sign = chr('+') | chr('-')
  def optSign = opt(sign) ^^ {
    case None => ""
    case Some(sign) => sign
  }
  def fraction = '.' ~ rep(digit) ^^ {
    case dot ~ ff => dot :: (ff mkString "") :: Nil mkString ""
  }
  def optFraction = opt(fraction) ^^ {
    case None => ""
    case Some(fraction) => fraction
  }
  def exponent = (chr('e') | chr('E')) ~ optSign ~ rep1(digit) ^^ {
    case e ~ optSign ~ exp => e :: optSign :: (exp mkString "") :: Nil mkString ""
  }
  def optExponent = opt(exponent) ^^ {
    case None => ""
    case Some(exponent) => exponent
  }
}

class FilterLexical extends StdLexical {
  // see `token` in `Scanners`
  override def token: Parser[Token] =
    ( delim
    | unquotedWordChar ~ rep( unquotedWordChar )  ^^
       { case first ~ rest => processIdent(first :: rest mkString "") }
    | '\"' ~ rep( quotedWordChar ) ~ '\"' ^^
       { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
    | EofCh ^^^ EOF
    | '\"' ~> failure("unclosed string literal")
    | failure("illegal character")
    )

  def isPrintable(ch: Char) =
     !ch.isControl && !ch.isSpaceChar && !ch.isWhitespace && ch != EofCh
  def isPrintableNonDelim(ch: Char) =
     isPrintable(ch) && ch != '(' && ch != ')'
  def unquotedWordChar = elem("unquoted word char",
     ch => ch != '"' && isPrintableNonDelim(ch))
  def quotedWordChar = elem("quoted word char",
     ch => ch != '"' && ch != '\n' && ch != EofCh)

 // // see `whitespace in `Scanners`
 // def whitespace: Parser[Any] = rep(
 //     whitespaceChar
 // //  | '/' ~ '/' ~ rep( chrExcept(EofCh, '\n') )
 //   )

  override protected def processIdent(name: String) =
    if (reserved contains name) Keyword(name) else StringLit(name)
}

object FilterParser extends StandardTokenParsers {
  override val lexical = new FilterLexical
  lexical.reserved ++= List("AND", "OR", "NOT")
  // lexical.delimiters ++= List("&","|","!","(",")")
  lexical.delimiters ++= List("(",")")

  def word = stringLit ^^ { s => EConst(Seq(s)) }

  def words =
    word.+ ^^ { x => EConst(x.flatMap(_ match { case EConst(y) => y })) }


  def parens: Parser[Expr] = "(" ~> expr <~ ")"

  def not: Parser[ENot] = "NOT" ~> term ^^ { ENot(_) }

  def term = ( words | parens | not )

  def andexpr = term * (
    "AND" ^^^ { (a:Expr, b:Expr) => EAnd(a,b) } )

  def orexpr = andexpr * (
    "OR" ^^^ { (a:Expr, b:Expr) => EOr(a,b) } )

  def expr = ( orexpr | term )

  def parse(s :String) = {
    val tokens = new lexical.Scanner(s)
    phrase(expr)(tokens)
  }

  def apply(s: String): Expr = {
    parse(s) match {
      case Success(tree, _) => tree
      case e: NoSuccess =>
        throw new IllegalArgumentException("Bad syntax: "+s)
    }
  }

  def test(exprstr: String, tweet: Seq[String]) = {
    parse(exprstr) match {
      case Success(tree, _) =>
        println("Tree: "+tree)
        val v = tree.matches(tweet)
        println("Eval: "+v)
      case e: NoSuccess => Console.err.println(e)
    }
  }
  
  //A main method for testing
  def main(args: Array[String]) = test(args(0), args(1).split("""\s"""))
}
