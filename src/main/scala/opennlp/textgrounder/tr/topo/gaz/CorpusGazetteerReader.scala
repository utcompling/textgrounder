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
package opennlp.textgrounder.tr.topo.gaz

import java.util.Iterator
import scala.collection.JavaConversions._

import opennlp.textgrounder.tr.text.Corpus
import opennlp.textgrounder.tr.text.Token
import opennlp.textgrounder.tr.topo.Location

class CorpusGazetteerReader(private val corpus: Corpus[_ <: Token])
  extends GazetteerReader {

  private val it = corpus.flatMap(_.flatMap {
    _.getToponyms.flatMap(_.getCandidates)
  }).toIterator

  def hasNext: Boolean = it.hasNext
  def next: Location = it.next

  def close() {
    corpus.close()
  }
}

