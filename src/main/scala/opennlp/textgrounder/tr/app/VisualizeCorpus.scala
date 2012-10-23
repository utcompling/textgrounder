package opennlp.textgrounder.tr.app

import processing.core._
//import processing.opengl._
//import codeanticode.glgraphics._
import de.fhpotsdam.unfolding._
import de.fhpotsdam.unfolding.geo._
import de.fhpotsdam.unfolding.events._
import de.fhpotsdam.unfolding.utils._
import de.fhpotsdam.unfolding.providers.Microsoft
import controlP5._

import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.io._
import opennlp.textgrounder.tr.text.prep._
import opennlp.textgrounder.tr.util._

import java.io._

import scala.collection.JavaConversions._

class VisualizeCorpus extends PApplet {

  class TopoMention(val toponym:Toponym,
                    val context:String,
                    val docid:String)

  var mapDetail:de.fhpotsdam.unfolding.Map = null
  var topoTextArea:Textarea = null

  val INIT_WIDTH = 1024
  val INIT_HEIGHT = 768

  val BORDER_WIDTH = 10
  val TEXTAREA_WIDTH = 185
  var textareaHeight = INIT_HEIGHT - BORDER_WIDTH*2
  var mapWidth = INIT_WIDTH - TEXTAREA_WIDTH - BORDER_WIDTH*3
  var mapHeight = INIT_HEIGHT - BORDER_WIDTH*2
  var textareaX = mapWidth+BORDER_WIDTH*2

  val CONTEXT_SIZE = 20

  val coordsMap = new scala.collection.mutable.HashMap[(Float, Float), List[TopoMention]]

  var oldWidth = INIT_WIDTH
  var oldHeight = INIT_HEIGHT

  override def setup {
    size(INIT_WIDTH, INIT_HEIGHT/*, GLConstants.GLGRAPHICS*/)
    //frame.setResizable(true)
    frame.setTitle("Corpus Visualizer")
    //textMode(PConstants.SHAPE)

    val cp5 = new ControlP5(this)

    mapDetail = new de.fhpotsdam.unfolding.Map(this, "detail", BORDER_WIDTH, BORDER_WIDTH, mapWidth, mapHeight/*, true, false, new Microsoft.AerialProvider*/)
    mapDetail.zoomToLevel(4)
    mapDetail.setZoomRange(2, 10)
    //mapDetail.zoomAndPanTo(new Location(25.0f, 12.0f), 2) // map center
    //mapDetail.zoomAndPanTo(new Location(38.5f, -98.0f), 2) // USA center
    val eventDispatcher = MapUtils.createDefaultEventDispatcher(this, mapDetail)

    topoTextArea = cp5.addTextarea("")
                      .setPosition(textareaX, BORDER_WIDTH)
                      .setSize(TEXTAREA_WIDTH, textareaHeight)
                      .setFont(createFont("arial",12))
                      .setLineHeight(14)
                      .setColor(color(0))

    val tokenizer = new OpenNLPTokenizer
    val corpus = TopoUtil.readStoredCorpusFromSerialized(VisualizeCorpus.inputFile)

    for(doc <- corpus) {
      val docArray = TextUtil.getDocAsArray(doc)
      var tokIndex = 0
      for(token <- docArray) {
        if(token.isToponym) {
          val toponym = token.asInstanceOf[Toponym]
          if(toponym.getAmbiguity > 0 && toponym.hasSelected) {
            val coord = toponym.getSelected.getRegion.getCenter
            val pair = (coord.getLatDegrees.toFloat, coord.getLngDegrees.toFloat)
            val prevList = coordsMap.getOrElse(pair, Nil)
            val context = TextUtil.getContext(docArray, tokIndex, CONTEXT_SIZE)
            coordsMap.put(pair, (prevList ::: (new TopoMention(toponym, context, doc.getId) :: Nil)))
          }
        }
        tokIndex += 1
      }
    }

  }

  val RADIUS = 10
  var selectedCirc:(Float, Float, List[TopoMention]) = null
  var oldSelectedCirc = selectedCirc
  var onScreen:List[((Float, Float), List[TopoMention])] = Nil
  val sb = new StringBuffer

  override def draw {
    background(255)

    /*if(width != oldWidth || height != oldHeight) {
      mapWidth = width - TEXTAREA_WIDTH - BORDER_WIDTH*3
      mapHeight = height - BORDER_WIDTH*2
      textareaX = mapWidth+BORDER_WIDTH*2
      textareaHeight = mapHeight

      mapDetail.mapDisplay.resize(mapWidth, mapHeight)
      topoTextArea.setPosition(textareaX, BORDER_WIDTH)
                  .setSize(TEXTAREA_WIDTH, textareaHeight)

      oldWidth = width
      oldHeight = height
    }*/

    mapDetail.draw

    onScreen =
    (for(((lat,lng),topolist) <- coordsMap) yield {
      val ufLoc:de.fhpotsdam.unfolding.geo.Location = new de.fhpotsdam.unfolding.geo.Location(lat, lng)
      val xy:Array[Float] = mapDetail.getScreenPositionFromLocation(ufLoc)
      if(xy(0) >= BORDER_WIDTH + RADIUS && xy(0) <= mapWidth + BORDER_WIDTH - RADIUS
         && xy(1) >= BORDER_WIDTH + RADIUS && xy(1) <= mapHeight + BORDER_WIDTH - RADIUS) {
        if(selectedCirc != null && lat == selectedCirc._1 && lng == selectedCirc._2)
          fill(200, 0, 0, 100)
        else
          fill(0, 200, 0, 100)
        ellipse(xy(0), xy(1), RADIUS*2, RADIUS*2)
        fill(1)
        text(topolist.size, xy(0)-RADIUS/4, xy(1)+RADIUS/4)

        Some(((lat,lng),topolist))
      }
      else
        None
    }).flatten.toList

    if(selectedCirc != oldSelectedCirc) {
      if(selectedCirc != null) {
        sb.setLength(0)
        sb.append(selectedCirc._3(0).toponym.getOrigForm)
        var i = 1
        for(topoMention <- selectedCirc._3) {
          sb.append("\n\n")
          sb.append(i)
          sb.append(". ")
          if(!topoMention.context.startsWith("[[")) sb.append("...")
          sb.append(topoMention.context)
          if(!topoMention.context.endsWith("]]")) sb.append("...")
          sb.append(" (")
          sb.append(topoMention.docid)
          sb.append(")")
            i += 1
        }
        topoTextArea.setText(sb.toString)
      }
      else
        topoTextArea.setText("")
    }
    
    oldSelectedCirc = selectedCirc
  }

  var mousePressedX = -1.0
  var mousePressedY = -1.0
  override def mousePressed {
    mousePressedX = mouseX
    mousePressedY = mouseY
  }

  override def mouseReleased {

    var clickedCirc = false
    for(((lat, lng), topolist) <- onScreen) {
      val xy:Array[Float] = mapDetail.getScreenPositionFromLocation(new de.fhpotsdam.unfolding.geo.Location(lat, lng))
      if(PApplet.dist(mouseX, mouseY, xy(0), xy(1)) <= RADIUS) {
        oldSelectedCirc = selectedCirc
        selectedCirc = (lat, lng, topolist)
        clickedCirc = true
      }
    }
    if(mouseX == mousePressedX && mouseY == mousePressedY) {
      if(selectedCirc != null/* && PApplet.dist(mouseX, mouseY, selectedCirc._1, selectedCirc._2) > RADIUS*/)
        topoTextArea.scroll(0)
      if(!clickedCirc) {
        oldSelectedCirc = selectedCirc
        selectedCirc = null
      }
    }
  }


}

object VisualizeCorpus extends PApplet {

  var inputFile:String = null

  def main(args:Array[String]) {
    inputFile = args(0)
    PApplet.main(Array(/*"--present", */"opennlp.textgrounder.tr.app.VisualizeCorpus"))
  }
}
