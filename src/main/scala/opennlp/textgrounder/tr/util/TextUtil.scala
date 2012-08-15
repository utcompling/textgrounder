package opennlp.textgrounder.tr.util

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.text.io._

import scala.collection.JavaConversions._

object TextUtil {

  val alphanumRE = """^[a-zA-Z0-9 ]+$""".r

  def getDocAsArray(doc:Document[Token]): Array[Token] = {
    (for(sent <- doc) yield {
      (for(token <- sent.filter(t => alphanumRE.findFirstIn(t.getForm) != None)) yield {
        token
      })
    }).flatten.toArray
  }

  def getDocAsArray(doc:Document[StoredToken]): Array[StoredToken] = {
    (for(sent <- doc) yield {
      (for(token <- sent.filter(t => alphanumRE.findFirstIn(t.getForm) != None)) yield {
        token
      })
    }).flatten.toArray
  }

  def getContextFeatures(docAsArray:Array[Token], tokIndex:Int, windowSize:Int, stoplist:Set[String]): Array[String] = {
    val startIndex = math.max(0, tokIndex - windowSize)
    val endIndex = math.min(docAsArray.length, tokIndex + windowSize + 1)

    Array.concat(docAsArray.slice(startIndex, tokIndex).map(_.getForm),
                 docAsArray.slice(tokIndex + 1, endIndex).map(_.getForm)).filterNot(stoplist(_))
  }

  def getContextFeatures(docAsArray:Array[StoredToken], tokIndex:Int, windowSize:Int, stoplist:Set[String]): Array[String] = {
    val startIndex = math.max(0, tokIndex - windowSize)
    val endIndex = math.min(docAsArray.length, tokIndex + windowSize + 1)

    Array.concat(docAsArray.slice(startIndex, tokIndex).map(_.getForm),
                 docAsArray.slice(tokIndex + 1, endIndex).map(_.getForm)).filterNot(stoplist(_))
  }

  def getContext(docAsArray:Array[Token], tokIndex:Int, windowSize:Int): String = {
    val startIndex = math.max(0, tokIndex - windowSize)
    val endIndex = math.min(docAsArray.length, tokIndex + windowSize + 1)

    (docAsArray.slice(startIndex, tokIndex).map(_.getOrigForm).mkString("", " ", "") +
    " [["+docAsArray(tokIndex).getOrigForm+"]] " +
    docAsArray.slice(tokIndex + 1, endIndex).map(_.getOrigForm).mkString("", " ", "")).trim
  }

  def getContext(docAsArray:Array[StoredToken], tokIndex:Int, windowSize:Int): String = {
    val startIndex = math.max(0, tokIndex - windowSize)
    val endIndex = math.min(docAsArray.length, tokIndex + windowSize + 1)

    (docAsArray.slice(startIndex, tokIndex).map(_.getOrigForm).mkString("", " ", "") +
    " [["+docAsArray(tokIndex).getOrigForm+"]] " +
    docAsArray.slice(tokIndex + 1, endIndex).map(_.getOrigForm).mkString("", " ", "")).trim
  }

  def stripPunc(s: String): String = {
    var toReturn = s.trim
    while(toReturn.length > 0 && !Character.isLetterOrDigit(toReturn.charAt(0)))
      toReturn = toReturn.substring(1)
    while(toReturn.length > 0 && !Character.isLetterOrDigit(toReturn.charAt(toReturn.length-1)))
      toReturn = toReturn.substring(0,toReturn.length()-1)
    toReturn
  }

}
