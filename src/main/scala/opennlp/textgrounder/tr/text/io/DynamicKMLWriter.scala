package opennlp.textgrounder.tr.text.io

import java.io._
import java.util._
import javax.xml.datatype._
import javax.xml.stream._

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._

import scala.collection.JavaConversions._

class DynamicKMLWriter(val corpus:StoredCorpus/*,
                       val outputGoldLocations:Boolean = false*/) {

  lazy val factory = XMLOutputFactory.newInstance

  val CONTEXT_SIZE = 20

  def write(out:XMLStreamWriter) {
    KMLUtil.writeHeader(out, "corpus")

    var globalTokIndex = 0
    var globalTopIndex = 1
    for(doc <- corpus) {
      val docArray = TextUtil.getDocAsArray(doc)
      var tokIndex = 0
      for(token <- docArray) {
        if(token.isToponym) {
          val toponym = token.asInstanceOf[Toponym]
          if(toponym.getAmbiguity > 0 && toponym.hasSelected) {
            val coord = toponym.getSelected.getRegion.getCenter
            val context = TextUtil.getContext(docArray, tokIndex, CONTEXT_SIZE)
            KMLUtil.writePinTimeStampPlacemark(out, toponym.getOrigForm, coord, context, globalTopIndex)
            globalTopIndex += 1
          }
        }
        tokIndex += 1
        globalTokIndex += 1
      }
    }

    KMLUtil.writeFooter(out)
    out.close
  }

  def write(file:File) {
    val stream = new BufferedOutputStream(new FileOutputStream(file))
    this.write(this.factory.createXMLStreamWriter(stream, "UTF-8"))
    stream.close()
  }
}
