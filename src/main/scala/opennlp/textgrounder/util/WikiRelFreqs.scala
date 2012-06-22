///////////////////////////////////////////////////////////////////////////////
//  WikiRelFreqs.scala
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

package opennlp.textgrounder.util

object WikiRelFreqs extends App {

  val geoFreqs = getFreqs(args(0))
  val allFreqs = getFreqs(args(1))

  val relFreqs = allFreqs.map(p => (p._1, geoFreqs.getOrElse(p._1, 0.0) / p._2)).toList.sortWith((x, y) => if(x._2 != y._2) x._2 > y._2 else x._1 < y._1)

  relFreqs.foreach(println)

  def getFreqs(filename:String):Map[String, Double] = {
    val wordCountRE = """^(\w+)\s=\s(\d+)$""".r
    val lines = scala.io.Source.fromFile(filename).getLines
    val freqs = new scala.collection.mutable.HashMap[String, Long]
    var total = 0l
    var lineCount = 0

    for(line <- lines) {
      if(wordCountRE.findFirstIn(line) != None) {
        val wordCountRE(word, count) = line
        val lowerWord = word.toLowerCase
        val oldCount = freqs.getOrElse(lowerWord, 0l)
        freqs.put(lowerWord, oldCount + count.toInt)
        total += count.toInt
      }
      if(lineCount % 10000000 == 0)
        println(filename+" "+lineCount)
      lineCount += 1
    }

    freqs.map(p => (p._1, p._2.toDouble / total)).toMap
  }
}
