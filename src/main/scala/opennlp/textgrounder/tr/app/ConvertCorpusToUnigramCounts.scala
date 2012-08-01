package opennlp.textgrounder.tr.app

import java.io._
import java.util.zip._

import opennlp.textgrounder.tr.util._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.topo.gaz._
import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.text.io._

import scala.collection.JavaConversions._

object ConvertCorpusToUnigramCounts extends BaseApp {

  val alphanumRE = """^[a-z0-9]+$""".r

  //val tokenizer = new OpenNLPTokenizer

  def main(args:Array[String]) { 

    initializeOptionsFromCommandLine(args);

    /*var corpus = Corpus.createStoredCorpus

    if(getCorpusFormat == BaseApp.CORPUS_FORMAT.PLAIN/**/) {
      /*
      val tokenizer = new OpenNLPTokenizer
      //val recognizer = new OpenNLPRecognizer
      //val gis = new GZIPInputStream(new FileInputStream(args(1)))
      //val ois = new ObjectInputStream(gis)
      //val gnGaz = ois.readObject.asInstanceOf[GeoNamesGazetteer]
      //gis.close
      corpus.addSource(new PlainTextSource(
        new BufferedReader(new FileReader(args(0))), new OpenNLPSentenceDivider(), tokenizer))
      //corpus.addSource(new ToponymAnnotator(new PlainTextSource(
      //		new BufferedReader(new FileReader(args(0))), new OpenNLPSentenceDivider(), tokenizer),
      //          recognizer, gnGaz, null))
      corpus.setFormat(BaseApp.CORPUS_FORMAT.PLAIN)
      */
      val importCorpus = new ImportCorpus
      //if(args(0).endsWith("txt"))
      corpus = importCorpus.doImport(getCorpusInputPath, , getCorpusFormat, false)
      //else
      //  corpus = importCorpus
    }
    else if(getCorpusFormat == BaseApp.CORPUS_FORMAT.TRCONLL) {
      corpus.addSource(new TrXMLDirSource(new File(args(0)), tokenizer))
      corpus.setFormat(BaseApp.CORPUS_FORMAT.TRCONLL)
      corpus.load
    }
    //corpus.load*/

    val corpus = TopoUtil.readStoredCorpusFromSerialized(getSerializedCorpusInputPath)
    
    System.err.println(corpus.size)
    
    var i = 0
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
    
      print(i/*doc.getId.drop(1)*/ +"\t")
      print(doc.getId+"\t")
      print("0,0\t")
      print("1\t\tMain\tno\tno\tno\t")
      for((word, count) <- unigramCounts) {
        print(word+":"+count+" ")
      }
      println
      i += 1
    }

  }
}
