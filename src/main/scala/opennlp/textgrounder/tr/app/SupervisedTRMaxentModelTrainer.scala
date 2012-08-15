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

import org.apache.tools.bzip2._
import org.clapper.argot._
import ArgotConverters._

import opennlp.maxent._
import opennlp.maxent.io._
import opennlp.model._

object SupervisedTRFeatureExtractor extends App {
  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.tr.app.SupervisedTRMaxentModelTrainer", preUsage = Some("Textgrounder"))

  val wikiCorpusInputFile = parser.option[String](List("c", "corpus"), "corpus", "wiki training corpus input file")
  val wikiTextInputFile = parser.option[String](List("w", "wiki"), "wiki", "wiki text input file")
  val trInputFile = parser.option[String](List("i", "tr-input"), "tr-input", "TR-CoNLL input path")
  val gazInputFile = parser.option[String](List("g", "gaz"), "gaz", "serialized gazetteer input file")
  val stoplistInputFile = parser.option[String](List("s", "stoplist"), "stoplist", "stopwords input file")
  val modelsOutputDir = parser.option[String](List("d", "models-dir"), "models-dir", "models output directory")
  val thresholdParam = parser.option[Double](List("t", "threshold"), "threshold", "maximum distance threshold")

  val windowSize = 20
  val dpc = 1.0
  val threshold = if(thresholdParam.value != None) thresholdParam.value.get else 1.0

  try {
    parser.parse(args)
  }
  catch {
    case e: ArgotUsageException => println(e.message); sys.exit(0)
  }

  println("Reading toponyms from TR-CoNLL at " + trInputFile.value.get + " ...")
  val toponyms:Set[String] = CorpusInfo.getCorpusInfo(trInputFile.value.get).map(_._1).toSet

  toponyms.foreach(println)

  println("Reading Wikipedia geotags from " + wikiCorpusInputFile.value.get + "...")
  val idsToCoords = new collection.mutable.HashMap[String, Coordinate]
  val fis = new FileInputStream(wikiCorpusInputFile.value.get)
  fis.read; fis.read
  val cbzis = new CBZip2InputStream(fis)
  val in = new BufferedReader(new InputStreamReader(cbzis))
  var curLine = in.readLine
  while(curLine != null) {
    val tokens = curLine.split("\t")
    val coordTokens = tokens(2).split(",")
    idsToCoords.put(tokens(0), Coordinate.fromDegrees(coordTokens(0).toDouble, coordTokens(1).toDouble))
    curLine = in.readLine
  }
  in.close
 
  println("Reading serialized gazetteer from " + gazInputFile.value.get + " ...")
  val gis = new GZIPInputStream(new FileInputStream(gazInputFile.value.get))
  val ois = new ObjectInputStream(gis)
  val gnGaz = ois.readObject.asInstanceOf[GeoNamesGazetteer]
  gis.close

  println("Reading Wiki text corpus from " + wikiTextInputFile.value.get + " ...")

  val recognizer = new OpenNLPRecognizer
  val tokenizer = new OpenNLPTokenizer

  val wikiTextCorpus = Corpus.createStreamCorpus
  
  wikiTextCorpus.addSource(new ToponymAnnotator(new WikiTextSource(new BufferedReader(new FileReader(wikiTextInputFile.value.get))), recognizer, gnGaz))
  wikiTextCorpus.setFormat(BaseApp.CORPUS_FORMAT.WIKITEXT)

  val stoplist:Set[String] =
    if(stoplistInputFile.value != None) {
      println("Reading stopwords file from " + stoplistInputFile.value.get + " ...")
      scala.io.Source.fromFile(stoplistInputFile.value.get).getLines.toSet
    }
    else {
      println("No stopwords file specified. Using an empty stopword list.")
      Set()
    }

  println("Building training sets for each toponym type...")

  val toponymsToTrainingSets = new collection.mutable.HashMap[String, List[(Array[String], String)]]
  for(doc <- wikiTextCorpus) {
    if(idsToCoords.containsKey(doc.getId)) {
      val docCoord = idsToCoords(doc.getId)
      println(doc.getId+" has a geotag: "+docCoord)
      val docAsArray = TextUtil.getDocAsArray(doc)
      var tokIndex = 0
      for(token <- docAsArray) {
        if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0) {
          if(toponyms(token.getForm))
            println(token.getForm+" is a toponym we care about.")
          else
            println(token.getForm+" is a toponym, but we don't care about it.")
        }
        if(token.isToponym && token.asInstanceOf[Toponym].getAmbiguity > 0 && toponyms(token.getForm)) {
          val toponym = token.asInstanceOf[Toponym]
          val bestCellNum = getBestCellNum(toponym, docCoord, threshold, dpc)
          if(bestCellNum != -1) {
            val contextFeatures = TextUtil.getContextFeatures(docAsArray, tokIndex, windowSize, stoplist)
            val prevSet = toponymsToTrainingSets.getOrElse(token.getForm, Nil)
            print(toponym+": ")
            contextFeatures.foreach(f => print(f+","))
            println(bestCellNum)
            
            toponymsToTrainingSets.put(token.getForm, (contextFeatures, bestCellNum.toString) :: prevSet)
          }
        }
        tokIndex += 1
      }
    }
    else {
      println(doc.getId+" does not have a geotag.")
      for(sent <- doc) { for(token <- sent) {} }
    }
  }

  val dir =
    if(modelsOutputDir.value.get != None) {
      println("Training Maxent models for each toponym type, outputting to directory " + modelsOutputDir.value.get + " ...")
      val dirFile:File = new File(modelsOutputDir.value.get)
      if(!dirFile.exists)
        dirFile.mkdir
      if(modelsOutputDir.value.get.endsWith("/"))
        modelsOutputDir.value.get
      else
        modelsOutputDir.value.get+"/"
    }
    else {
      println("Training Maxent models for each toponym type, outputting to current working directory ...")
      ""
    }
  for((toponym, trainingSet) <- toponymsToTrainingSets) {
    val outFile = new File(dir + toponym.replaceAll(" ", "_")+".txt")
    val out = new BufferedWriter(new FileWriter(outFile))
    for((context, label) <- trainingSet) {
      for(feature <- context) out.write(feature+",")
      out.write(label+"\n")
    }
    out.close
    /*val model = GIS.trainModel(MaxentEventStreamFactory(trainingSet.toIterator), iterations, cutoff)
    val modelWriter = new BinaryGISModelWriter(model, new File(dir + toponym.replaceAll(" ", "_")+".mxm"))
    modelWriter.persist()
    modelWriter.close()*/
  }

  println("All done.")

  def getBestCellNum(toponym:Toponym, docCoord:Coordinate, threshold:Double, dpc:Double): Int = {
    for(loc <- toponym.getCandidates) {
      if(loc.getRegion.distanceInKm(new PointRegion(docCoord)) < threshold) {
        return TopoUtil.getCellNumber(loc.getRegion.getCenter, dpc)
      }
    }
    -1
  }
  
}

object SupervisedTRMaxentModelTrainer extends App {

  val iterations = 10
  val cutoff = 2

  val dir = new File(args(0))
  for(file <- dir.listFiles.filter(_.getName.endsWith(".txt"))) {
    try {
      val reader = new BufferedReader(new FileReader(file))
      val dataStream = new PlainTextByLineDataStream(reader)
      val eventStream = new BasicEventStream(dataStream, ",")

      //GIS.PRINT_MESSAGES = false
      val model = GIS.trainModel(eventStream, iterations, cutoff)
      val modelWriter = new BinaryGISModelWriter(model, new File(file.getAbsolutePath.replaceAll(".txt", ".mxm")))
      modelWriter.persist()
      modelWriter.close()
    } catch {
      case e: Exception => e.printStackTrace
    }
  }
}

object MaxentEventStreamFactory {
  def apply(iterator:Iterator[(Array[String], String)]): EventStream = {
    new BasicEventStream(new DataStream {
      def nextToken: AnyRef = {
        val next = iterator.next
        val featuresAndLabel = (next._1.toList ::: (next._2 :: Nil)).mkString(",")
        println(featuresAndLabel)
        featuresAndLabel
      }
      def hasNext: Boolean = iterator.hasNext
    }, ",")
  }
}
