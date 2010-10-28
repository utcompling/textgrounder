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
package opennlp.textgrounder.text.io

import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.util.ArrayList
import java.util.List
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

import opennlp.textgrounder.text._
import opennlp.textgrounder.text.prep._

class GigawordSource(
  reader: BufferedReader,
  private val sentencesPerDocument: Int,
  private val numberOfDocuments: Int)
  extends TextSource(reader) {

  def this(reader: BufferedReader, sentencesPerDocument: Int) =
    this(reader, sentencesPerDocument, Int.MaxValue)
  def this(reader: BufferedReader) = this(reader, 50)

  val sentences = new Iterator[Sentence[Token]] {
    var current = GigawordSource.this.readLine
    def hasNext: Boolean = current != null
    def next: Sentence[Token] = new Sentence[Token](null) {
      val buffer = Buffer(new SimpleToken(current))
      current = GigawordSource.this.readLine
      while (current.trim.length > 0) {
        buffer += new SimpleToken(current)
        current = GigawordSource.this.readLine
      }
      current = GigawordSource.this.readLine

      def tokens: java.util.Iterator[Token] = buffer.toIterator
    }
  }.grouped(sentencesPerDocument).take(numberOfDocuments)

  def hasNext: Boolean = sentences.hasNext

  def next: Document[Token] = new Document[Token](null) {
    def iterator: java.util.Iterator[Sentence[Token]] =
      sentences.next.toIterator
  }
}

