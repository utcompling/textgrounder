package opennlp.textgrounder.tr.app

import java.io._

import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.text.io._

import scala.collection.JavaConversions._

object ConvertCorpusToUnigramCounts extends App {

  val alphanumRE = """^[a-z0-9]+$""".r

  val tokenizer = new OpenNLPTokenizer

  val corpus = Corpus.createStoredCorpus
  corpus.addSource(new TrXMLDirSource(new File(args(0)), tokenizer))
  corpus.setFormat(BaseApp.CORPUS_FORMAT.TRCONLL)
  corpus.load

  for(doc <- corpus) {
    val unigramCounts = new collection.mutable.HashMap[String, Int]
    for(sent <- doc) {
      for(rawToken <- sent) {
        for(token <- rawToken.getForm.split(" ")) {
          val ltoken = token.toLowerCase
          if(alphanumRE.findFirstIn(ltoken) != None) {
            val prevCount = unigramCounts.getOrElse(ltoken, 0)
            unigramCounts.put(ltoken, prevCount + 1)
          }
        }
      }
    }

    print(doc.getId.drop(1)+"\t")
    print(doc.getId+"\t")
    print("0,0\t")
    print("1\t\tMain\tno\tno\tno\t")
    for((word, count) <- unigramCounts) {
      print(word+":"+count+" ")
    }
    println
  }
}
