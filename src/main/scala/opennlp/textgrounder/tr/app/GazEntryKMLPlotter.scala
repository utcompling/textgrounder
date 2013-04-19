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

object GazEntryKMLPlotter /*extends BaseApp*/ {
  
  def main(args:Array[String]) {

    val toponym = args(0).replaceAll("_", " ")
    //val gaz = println("Reading serialized gazetteer from " + args(1) + " ...")
    val gis = new GZIPInputStream(new FileInputStream(args(1)))
    val ois = new ObjectInputStream(gis)
    val gnGaz = ois.readObject.asInstanceOf[GeoNamesGazetteer]
    gis.close

    val entries = gnGaz.lookup(toponym)
    if(entries != null) {
      var loc = entries(0)
      for(entry <- entries)
        if(entry.getRegion.getRepresentatives.size > 1)
          loc = entry
      if(loc != null)
      for(coord <- loc.getRegion.getRepresentatives) {
        println("<Placemark>")
        println("<styleUrl>#My_Style</styleUrl>")
        println("<Point>")
        println("<coordinates>"+coord.getLngDegrees+","+coord.getLatDegrees+",0</coordinates>")
        println("</Point>")
        println("</Placemark>")
      }
    }

    /*initializeOptionsFromCommandLine(args)

    val corpus = TopoUtil.readStoredCorpusFromSerialized(getSerializedCorpusInputPath)

    for(doc <- corpus) {
      for(sent <- doc) {
        for(toponym <- sent.getToponyms.filter(_.getAmbiguity > 0)) {
          
        }
      }
    }*/
  }
}
