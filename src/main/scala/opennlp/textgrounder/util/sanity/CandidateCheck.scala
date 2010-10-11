///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
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
package opennlp.textgrounder.util.sanity

import java.io._
import scala.collection.JavaConversions._

import opennlp.textgrounder.text.Corpus
import opennlp.textgrounder.text.Toponym
import opennlp.textgrounder.text.io.TrXMLDirSource
import opennlp.textgrounder.text.prep.OpenNLPTokenizer
import opennlp.textgrounder.topo.Location

object CandidateCheck extends Application {
  override def main(args: Array[String]) {
    val tokenizer = new OpenNLPTokenizer
    val corpus = Corpus.createStreamCorpus
    val cands = scala.collection.mutable.Map[java.lang.String, java.util.List[Location]]()

    corpus.addSource(new TrXMLDirSource(new File(args(0)), tokenizer))
    corpus.foreach { _.foreach { _.getToponyms.foreach {
      case toponym: Toponym => {
        if (!cands.contains(toponym.getForm)) {
          //println("Doesn't contain: " + toponym.getForm)
          cands(toponym.getForm) = toponym.getCandidates
        } else {
          val prev = cands(toponym.getForm)
          val here = toponym.getCandidates
          //println("Contains: " + toponym.getForm)
          if (prev.size != here.size) {
            println("=====Size error for " + toponym.getForm + ": " + prev.size + " " + here.size)
          } else {
            prev.zip(here).foreach { case (p, h) =>
              println(p.getRegion.getCenter + " " + h.getRegion.getCenter)
              //case (p, h) if p != h => println("=====Mismatch for " + toponym.getForm)
              //case _ => ()
            }
          }
        }
      }
    }}}
  }
}

