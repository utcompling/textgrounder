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
import java.awt.event._

import scala.collection.JavaConversions._

class VisualizeCorpus extends PApplet {

  class TopoMention(val toponym:Toponym,
                    val context:String,
                    val docid:String)

  var mapDetail:de.fhpotsdam.unfolding.Map = null
  var topoTextArea:Textarea = null
  var checkbox:CheckBox = null

  var allButton:Button = null
  var noneButton:Button = null

  val INIT_WIDTH = if(VisualizeCorpus.inputWidth > 0) VisualizeCorpus.inputWidth else 1280
  val INIT_HEIGHT = if(VisualizeCorpus.inputHeight > 0) VisualizeCorpus.inputHeight else 720

  val RADIUS = 10

  val BORDER_WIDTH = 10
  val TEXTAREA_WIDTH = 185
  val BUTTONPANEL_WIDTH = 35
  val CHECKBOX_WIDTH = 185 - BUTTONPANEL_WIDTH
  var textareaHeight = INIT_HEIGHT - BORDER_WIDTH*2
  var mapWidth = INIT_WIDTH - TEXTAREA_WIDTH - CHECKBOX_WIDTH - BUTTONPANEL_WIDTH - BORDER_WIDTH*4
  var mapHeight = INIT_HEIGHT - BORDER_WIDTH*2
  var checkboxX = BORDER_WIDTH + BUTTONPANEL_WIDTH
  var checkboxHeight = textareaHeight
  var mapX = checkboxX + CHECKBOX_WIDTH + BORDER_WIDTH
  var textareaX = mapWidth + CHECKBOX_WIDTH + BUTTONPANEL_WIDTH + BORDER_WIDTH*3

  val SLIDER_WIDTH = 5

  var BUTTON_WIDTH = 27
  var BUTTON_HEIGHT = 15
  val NONE_BUTTON_Y = BORDER_WIDTH + BUTTON_HEIGHT + 10

  val CONTEXT_SIZE = 20

  var cp5:ControlP5 = null

  val coordsMap = new scala.collection.mutable.HashMap[(Float, Float), List[TopoMention]]
  var docList:Array[String] = null
  var shownDocs:Set[String] = null

  var oldWidth = INIT_WIDTH
  var oldHeight = INIT_HEIGHT

  var checkboxTotalHeight = 0

  override def setup {
    size(INIT_WIDTH, INIT_HEIGHT/*, GLConstants.GLGRAPHICS*/)
    //frame.setResizable(true)
    frame.setTitle("Corpus Visualizer")
    //textMode(PConstants.SHAPE)

    cp5 = new ControlP5(this)

    mapDetail = new de.fhpotsdam.unfolding.Map(this, "detail", mapX, BORDER_WIDTH, mapWidth, mapHeight/*, true, false, new Microsoft.AerialProvider*/)
    mapDetail.setZoomRange(2, 10)
    //mapDetail.zoomToLevel(4)
    mapDetail.zoomAndPanTo(new Location(25.0f, 12.0f), 2) // map center
    //mapDetail.zoomAndPanTo(new Location(38.5f, -98.0f), 2) // USA center
    val eventDispatcher = MapUtils.createDefaultEventDispatcher(this, mapDetail)

    topoTextArea = cp5.addTextarea("")
                      .setPosition(textareaX, BORDER_WIDTH)
                      .setSize(TEXTAREA_WIDTH, textareaHeight)
                      .setFont(createFont("arial",12))
                      .setLineHeight(14)
                      .setColor(color(0))

    allButton = cp5.addButton("all")
                   .setPosition(BORDER_WIDTH, BORDER_WIDTH)
                   .setSize(BUTTON_WIDTH, BUTTON_HEIGHT)

    noneButton = cp5.addButton("none")
                    .setPosition(BORDER_WIDTH, NONE_BUTTON_Y)
                    .setSize(BUTTON_WIDTH,  BUTTON_HEIGHT)

    checkbox = cp5.addCheckBox("checkBox")
                  .setPosition(checkboxX, BORDER_WIDTH)
                  .setColorForeground(color(120))
                  .setColorActive(color(0, 200, 0))
                  .setColorLabel(color(0))
                  .setSize(10, 10)
                  .setItemsPerRow(1)
                  .setSpacingColumn(30)
                  .setSpacingRow(10)

    cp5.addSlider("slider")
       .setPosition(checkboxX + CHECKBOX_WIDTH - SLIDER_WIDTH, BORDER_WIDTH)
       .setSize(SLIDER_WIDTH, checkboxHeight)
       .setRange(0, 1)
       .setValue(1)
       .setLabelVisible(false)
       .setSliderMode(Slider.FLEXIBLE)
       .setHandleSize(40)
       .setColorBackground(color(255))

    class myMWListener(vc:VisualizeCorpus) extends MouseWheelListener {
      def mouseWheelMoved(mwe:MouseWheelEvent) {
        vc.mouseWheel(mwe.getWheelRotation)
      }
    }

    addMouseWheelListener(new myMWListener(this))

    val tokenizer = new OpenNLPTokenizer
    val corpus = TopoUtil.readStoredCorpusFromSerialized(VisualizeCorpus.inputFile) //Stored

    docList =
    (for(doc <- corpus) yield {
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
      //checkbox.addItem(doc.getId, 0)
      doc.getId
    }).toArray

    docList.toList.sortBy(x => x).foreach(x => checkbox.addItem(x, 0))
    shownDocs = docList.toSet

    checkboxTotalHeight = docList.size * 20
    if(checkboxTotalHeight <= height)
      cp5.getController("slider").hide
    checkbox.activateAll
  }

  var selectedCirc:(Float, Float, List[TopoMention]) = null
  var oldSelectedCirc = selectedCirc
  var onScreen:List[((Float, Float), List[TopoMention])] = Nil
  val sb = new StringBuffer

  override def draw {
    background(255)

    mapDetail.draw

    checkbox.setPosition(checkboxX, BORDER_WIDTH -
                         ((1.0 - cp5.getController("slider").getValue) * (checkboxTotalHeight - checkboxHeight)).toInt)

    onScreen =
    (for(((lat,lng),rawTopolist) <- coordsMap) yield {
      val topolist = rawTopolist.filter(tm => shownDocs(tm.docid))
      if(topolist.size > 0) {
        val ufLoc:de.fhpotsdam.unfolding.geo.Location = new de.fhpotsdam.unfolding.geo.Location(lat, lng)
        val xy:Array[Float] = mapDetail.getScreenPositionFromLocation(ufLoc)
        if(xy(0) >= mapX + RADIUS && xy(0) <= mapX + mapWidth - RADIUS
           && xy(1) >= BORDER_WIDTH + RADIUS && xy(1) <= mapHeight + BORDER_WIDTH - RADIUS) {
             if(selectedCirc != null && lat == selectedCirc._1 && lng == selectedCirc._2)
               fill(200, 0, 0, 100)
             else
               fill(0, 200, 0, 100)
             ellipse(xy(0), xy(1), RADIUS*2, RADIUS*2)
             fill(1)
             val num = topolist.size
             if(num < 10)
               text(num, xy(0)-(RADIUS.toFloat/3.2).toInt, xy(1)+(RADIUS.toFloat/2.1).toInt)
             else
               text(num, xy(0)-(RADIUS.toFloat/1.3).toInt, xy(1)+(RADIUS.toFloat/2.1).toInt)
             
             Some(((lat,lng),topolist))
           }
           else
             None
      }
      else
        None
    }).flatten.toList

    if(selectedCirc != oldSelectedCirc) {
      if(selectedCirc != null) {
        val topoMentions = selectedCirc._3.filter(tm => shownDocs(tm.docid))
        sb.setLength(0)
        sb.append(selectedCirc._3(0).toponym.getOrigForm)
        sb.append(" (")
        sb.append(topoMentions.size)
        sb.append(")")
        var i = 1
        for(topoMention <- topoMentions) {
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

    if(mouseX >= mapX && mouseX <= mapX + mapWidth
       && mouseY >= BORDER_WIDTH && mouseY <= BORDER_WIDTH + mapHeight) { // clicked in map
      var clickedCirc = false
      for(((lat, lng), topolist) <- onScreen) {
        val xy:Array[Float] = mapDetail.getScreenPositionFromLocation(new de.fhpotsdam.unfolding.geo.Location(lat, lng))
        if(PApplet.dist(mouseX, mouseY, xy(0), xy(1)) <= RADIUS) {
          oldSelectedCirc = selectedCirc
          selectedCirc = (lat, lng, topolist)
          clickedCirc = true
        }
      }
      if(mouseX == mousePressedX && mouseY == mousePressedY) { // didn't drag
        if(selectedCirc != null/* && PApplet.dist(mouseX, mouseY, selectedCirc._1, selectedCirc._2) > RADIUS*/)
          topoTextArea.scroll(0)
        if(!clickedCirc) {
          oldSelectedCirc = selectedCirc
          selectedCirc = null
        }
      }
    }

    if(mouseX >= BORDER_WIDTH && mouseX <= checkboxX + CHECKBOX_WIDTH
       && mouseY >= 0 && mouseY <= BORDER_WIDTH + checkboxHeight) {
      shownDocs = checkbox.getItems.filter(_.getState == true).map(_.getLabel).toSet

      if(selectedCirc != null) {
        val topolist = coordsMap((selectedCirc._1, selectedCirc._2)).filter(tm => shownDocs(tm.docid))
        if(topolist.length > 0) {
          selectedCirc = (selectedCirc._1, selectedCirc._2, topolist)
        }
        else {
          oldSelectedCirc = selectedCirc
          selectedCirc = null
        }
      }
    }
  }

  def controlEvent(e:ControlEvent) {
    //try {
      if(mouseX >= BORDER_WIDTH && mouseX <= BORDER_WIDTH + BUTTON_WIDTH) {
        if(mouseY >= BORDER_WIDTH && mouseY <= BORDER_WIDTH + BUTTON_HEIGHT) {
          checkbox.activateAll
          shownDocs = docList.toSet
        }
        else if(mouseY >= NONE_BUTTON_Y && mouseY <= NONE_BUTTON_Y + BUTTON_HEIGHT) {
          checkbox.deactivateAll
          shownDocs.clear
        }
      }
    /*} catch {
      case e: Exception => //e.printStackTrace
    }*/
  }

  def mouseWheel(delta:Int) {
    if(mouseX >= BORDER_WIDTH && mouseX <= checkboxX + CHECKBOX_WIDTH - SLIDER_WIDTH
       && mouseY >= 0 && mouseY <= BORDER_WIDTH + checkboxHeight) {
      val slider = cp5.getController("slider")
      slider.setValue(slider.getValue - delta.toFloat / 100)
    }
    else if(mouseX >= textareaX && mouseX <= textareaX + TEXTAREA_WIDTH
       && mouseY >= BORDER_WIDTH && mouseY <= BORDER_WIDTH + textareaHeight) {
      topoTextArea.scrolled(delta * 2)
    }
  }

}

object VisualizeCorpus extends PApplet {

  var inputFile:String = null
  var inputWidth:Int = -1
  var inputHeight:Int = -1

  def main(args:Array[String]) {
    inputFile = args(0)
    if(args.size >= 3) {
      inputWidth = args(1).toInt
      inputHeight = args(2).toInt
    }
    PApplet.main(Array(/*"--present", */"opennlp.textgrounder.tr.app.VisualizeCorpus"))
  }
}
