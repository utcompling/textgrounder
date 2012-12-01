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
package opennlp.textgrounder.tr
package app

import java.io._

import eval._
import resolver._
import topo.gaz._
import text._
import text.io._
import text.prep._
import util.Constants

object ReprocessTrApp {
  def main(args: Array[String]) {
    val tokenizer = new OpenNLPTokenizer
    val recognizer = new OpenNLPRecognizer

    val gazetteer = new InMemoryGazetteer
    gazetteer.load(new WorldReader(new File(
      Constants.getGazetteersDir() + File.separator + "dataen-fixed.txt.gz"
    )))

    val corpus = Corpus.createStreamCorpus
    val source = new TrXMLDirSource(new File(args(0)), tokenizer)
    val stripped = new ToponymRemover(source)
    corpus.addSource(new ToponymAnnotator(stripped, recognizer, gazetteer))

    val writer = new CorpusXMLWriter(corpus)
    writer.write(new File(args(1)))
  }
}

