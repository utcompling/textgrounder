import scala.util.parsing.combinator.syntactical.StdTokenParsers
import scala.util.parsing.combinator.lexical.StdLexical


object ArithmeticParser extends StdTokenParsers with Application {
 type Tokens = StdLexical

 val lexical = new StdLexical

 lexical.delimiters ++= List("(", ")", "+", "-", "*", "/")

 def factor: Parser[Int] = "(" ~> expr <~ ")" | numericLit ^^ (_.toInt)

 def term : Parser[Int] = (
   factor ~ "*" ~ term ^^ { case x ~ "*" ~ y => x * y } |
   factor ~ "/" ~ term ^^ { case x ~ "/" ~ y => x / y } | factor )

def expr : Parser[Int] = (
  term ~ "+" ~ expr ^^ { case x ~ "+" ~ y => x + y } |
  term ~ "-" ~ expr ^^ { case x ~ "-" ~ y => x - y } | term )

  def apply(x: String) =
    //(expr (new lexical.Scanner ("1+2*3*7-1") ))
    (expr (new lexical.Scanner (x) ))
}


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

import scala.util.parsing.combinator.lexical.StdLexical

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

import scala.util.parsing.combinator.syntactical._

object ExprParser extends StandardTokenParsers {
    override val lexical = new ExprLexical
    lexical.delimiters ++= List("&","|","!","(",")")

    def word = numericLit ^^ { s => EConst(Seq(s)) }

    def words =
        word.+ ^^ { x => EConst(x.flatMap(_ match { case EConst(y) => y })) }


    def parens: Parser[Expr] = "(" ~> expr <~ ")"

    def not: Parser[ENot] = "!" ~> term ^^ { ENot(_) }

    def term = ( words | parens | not )

    def andexpr = term * (
            "&" ^^^ { (a:Expr, b:Expr) => EAnd(a,b) } )

    def orexpr = andexpr * (
            "|" ^^^ { (a:Expr, b:Expr) => EOr(a,b) } )

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
