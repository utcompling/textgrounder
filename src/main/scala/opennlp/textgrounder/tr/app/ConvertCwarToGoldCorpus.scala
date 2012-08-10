package opennlp.textgrounder.tr.app

import opennlp.textgrounder.tr.topo._
import opennlp.textgrounder.tr.util._
import opennlp.textgrounder.tr.topo.gaz._
import opennlp.textgrounder.tr.text.io._
import java.util.zip._

import java.io._

import scala.collection.JavaConversions._

object ConvertCwarToGoldCorpus extends App {

  val digitsRE = """^\d+$""".r
  val floatRE = """^-?\d*\.\d*$""".r
  val toponymRE = """^tgn,(\d+)-(.+)-]][^\s]*$""".r

  val corpusFiles = new File(args(0)).listFiles
  val goldKml = scala.xml.XML.loadFile(args(1))
  val gazIn = args(2)


  val tgnToCoord =
  //(goldKml \\ "Placemark").foreach { placemark =>
  (for(placemark <- (goldKml \\ "Placemark")) yield {
    var tgn = -1
    (placemark \\ "Data").foreach { data =>
      val name = (data \ "@name").text
      if(name.equals("tgn")) {
        val text = data.text.trim
        if(digitsRE.findFirstIn(text) != None)
          tgn = text.toInt
      }
    }
    var coordsRaw = ""
    (placemark \\ "coordinates").foreach { coordinates =>
      coordsRaw = coordinates.text.trim.dropRight(2)
    }

    val coordsSplit = coordsRaw.split(",")

    if(tgn == -1 || coordsSplit.size != 2 || floatRE.findFirstIn(coordsSplit(0)) == None
       || floatRE.findFirstIn(coordsSplit(1)) == None)
      None
    else {
      val lng = coordsSplit(0).toDouble
      val lat = coordsSplit(1).toDouble
      Some((tgn, Coordinate.fromDegrees(lat, lng)))
    }
  }).flatten.toMap
  
  //tgnToCoord.foreach(println)


  var gaz:GeoNamesGazetteer = null;
  //println("Reading serialized GeoNames gazetteer from " + gazIn + " ...")
  var ois:ObjectInputStream = null;
  if(gazIn.toLowerCase().endsWith(".gz")) {
    val gis = new GZIPInputStream(new FileInputStream(gazIn))
    ois = new ObjectInputStream(gis)
  }
  else {
    val fis = new FileInputStream(gazIn)
    ois = new ObjectInputStream(fis)
  }
  gaz = ois.readObject.asInstanceOf[GeoNamesGazetteer]

  println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>")
  println("<corpus>")

  for(file <- corpusFiles) {
    println("  <doc id=\""+file.getName+"\">")

    for(line <- scala.io.Source.fromFile(file).getLines.map(_.trim).filter(_.length > 0)) {
      println("    <s>")
      for(token <- line.split(" ")) {
        if(toponymRE.findFirstIn(token) != None) {
          val toponymRE(tgnRaw, formRaw) = token
          val form = formRaw.replaceAll("-", " ")
          val candidates = gaz.lookup(form.toLowerCase)
          val goldCoord = tgnToCoord.getOrElse(tgnRaw.toInt, null)
          if(candidates == null || goldCoord == null) {
            for(tok <- form.split(" ").filter(t => CorpusXMLWriter.isSanitary(/*BaseApp.CORPUS_FORMAT.PLAIN, */t)))
              println("      <w tok=\""+tok+"\"/>")
          }
          else {
            var matchingCand:Location = null
            for(cand <- candidates) {
              if(cand.getRegion.getCenter.distanceInMi(goldCoord) < 5.0) {
                matchingCand = cand
              }
            }
            if(matchingCand == null) {
              for(tok <- form.split(" ").filter(t => CorpusXMLWriter.isSanitary(/*BaseApp.CORPUS_FORMAT.PLAIN, */t)))
                println("      <w tok=\""+tok+"\"/>")
            }
            else {
              val formToWrite = if(CorpusXMLWriter.isSanitary(/*BaseApp.CORPUS_FORMAT.PLAIN, */form)) form else "MALFORMED"
              println("      <toponym term=\""+formToWrite+"\">")
              println("        <candidates>")
              for(cand <- candidates) {
                val region = cand.getRegion
                val center = region.getCenter
                print("          <cand id=\""+cand.getId+"\" lat=\""+center.getLatDegrees+"\" long=\""
                      +center.getLngDegrees+"\" type=\""+cand.getType+"\" admin1code=\""
                      +cand.getAdmin1Code+"\"")
                if(matchingCand == cand)
                  print(" selected=\"yes\"")
                println("/>")
                /*println("            <representatives>")
                for(rep <- region.getRepresentatives) {
                  println("              <rep lat=\""+rep.getLatDegrees+"\" long=\""+rep.getLngDegrees+"\"/>")
                }
                println("            </representatives>")*/
              }
              println("        </candidates>")
              println("      </toponym>")
            }
          }
        }
        else {
          val strippedToken = TextUtil.stripPunc(token)
          if(CorpusXMLWriter.isSanitary(/*BaseApp.CORPUS_FORMAT.PLAIN, */strippedToken))
            println("      <w tok=\""+strippedToken+"\"/>")
        }
      }
      println("    </s>")
    }
    
    println("  </doc>")
  }

  println("</corpus>")
}
