package opennlp.textgrounder.tr.util

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.text.io._

import scala.collection.JavaConversions._

object TextUtil {

  val alphanumRE = """^[a-zA-Z0-9]+$""".r

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

}
