///////////////////////////////////////////////////////////////////////////////
//  WordRankerByAvgErrorUT.scala
//
//  Copyright (C) 2012 Mike Speriosu, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////

package opennlp.textgrounder.postprocess

import opennlp.textgrounder.util.Twokenize
import org.clapper.argot._
import java.io._

object WordRankerByAvgErrorUT {

  import ArgotConverters._

  val parser = new ArgotParser("textgrounder run opennlp.textgrounder.postprocess.WordRankerByAvgError", preUsage = Some("TextGrounder"))
  val corpusFile = parser.option[String](List("i", "input"), "list", "corpus input file")
  val listFile = parser.option[String](List("l", "list"), "list", "list input file")
  val docThresholdOption = parser.option[Int](List("t", "threshold"), "threshold", "document frequency threshold")
  
  def main(args: Array[String]) {
    try {
      parser.parse(args)
    }
    catch {
      case e: ArgotUsageException => println(e.message); sys.exit(0)
    }

    if(corpusFile.value == None) {
      println("You must specify a corpus input file via -i.")
      sys.exit(0)
    }
    if(listFile.value == None) {
      println("You must specify a list input file via -l.")
      sys.exit(0)
    }

    val docThreshold = if(docThresholdOption.value == None) 10 else docThresholdOption.value.get

    val docNamesAndErrors:Map[String, Double] = scala.io.Source.fromFile(listFile.value.get).getLines.
      map(_.split("\t")).map(p => (p(0), p(1).toDouble)).toMap

    val in = new BufferedReader(
      new InputStreamReader(new FileInputStream(corpusFile.value.get), "UTF8"))

    val wordsToErrors = new scala.collection.mutable.HashMap[String, Double]
    val wordsToDocNames = new scala.collection.mutable.HashMap[String, scala.collection.immutable.HashSet[String]]

    var line:String = in.readLine

    while(line != null) {
      val tokens = line.split("\t")
      if(tokens.length >= 3) {
        val docName = tokens(0)
        if(docNamesAndErrors.contains(docName)) {
          val error = docNamesAndErrors(docName)
          val text = tokens(2)
          
          val wordsAndCounts:Map[String, Int] = text.split(" ").map(p => (p.split(":")(0), p.split(":")(1).toInt)).toMap
          val docSize = text.split(" ").map(_.split(":")(1).toInt).sum
          
          for((word, count) <- wordsAndCounts) {
            //if(!word.startsWith("@user_")) {
              val prevError = wordsToErrors.getOrElse(word, 0.0)
              wordsToErrors.put(word, prevError + error * count.toDouble/docSize)
              val prevSet = wordsToDocNames.getOrElse(word, new scala.collection.immutable.HashSet())
              wordsToDocNames.put(word, prevSet + docName)
            //}
          }
        }
      }
      line = in.readLine
    }  
    in.close

    wordsToErrors.foreach(p => if(wordsToDocNames(p._1).size < docThreshold) wordsToErrors.remove(p._1))

    wordsToErrors.foreach(p => wordsToErrors.put(p._1, p._2 / wordsToDocNames(p._1).size))

    wordsToErrors.toList.sortWith((x, y) => x._2 < y._2).foreach(p => println(p._1+"\t"+p._2))
  }
}
