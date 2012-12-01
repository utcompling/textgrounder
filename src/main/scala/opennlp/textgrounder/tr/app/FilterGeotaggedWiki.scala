package opennlp.textgrounder.tr
package app

import java.io._
import java.util.zip._

import util._
import topo._
import topo.gaz._
import text._
import text.prep._
import text.io._

import scala.collection.JavaConversions._

import org.apache.commons.compress.compressors.bzip2._
import org.clapper.argot._
import ArgotConverters._

object FilterGeotaggedWiki extends App {
  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.tr.app.FilterGeotaggedWiki", preUsage = Some("Textgrounder"))

  val wikiTextInputFile = parser.option[String](List("w", "wiki"), "wiki", "wiki text input file")
  val wikiCorpusInputFile = parser.option[String](List("c", "corpus"), "corpus", "wiki corpus input file")

  try {
    parser.parse(args)
  }
  catch {
    case e: ArgotUsageException => println(e.message); sys.exit(0)
  }

  val ids = new collection.mutable.HashSet[String]

  val fis = new FileInputStream(wikiCorpusInputFile.value.get)
  fis.read; fis.read
  val cbzis = new BZip2CompressorInputStream(fis)
  val in = new BufferedReader(new InputStreamReader(cbzis))
  var curLine = in.readLine
  while(curLine != null) {
    ids += curLine.split("\t")(0)
    curLine = in.readLine
  }
  in.close

  val wikiTextCorpus = Corpus.createStreamCorpus

  wikiTextCorpus.addSource(new WikiTextSource(new BufferedReader(new FileReader(wikiTextInputFile.value.get))))
  wikiTextCorpus.setFormat(BaseApp.CORPUS_FORMAT.WIKITEXT)

  for(doc <- wikiTextCorpus) {
    if(ids contains doc.getId) {
      println("Article title: " + doc.title)
      println("Article ID: " + doc.getId)
      for(sent <- doc) {
        for(token <- sent) {
          println(token.getOrigForm)
        }
      }
    }
    else {
      for(sent <- doc) { for(token <- sent) {} }
    }
  }
}
