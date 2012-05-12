package opennlp.textgrounder.tr.text.io

import java.io._

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import opennlp.textgrounder.tr.text._
import opennlp.textgrounder.tr.text.prep._

class WikiTextSource(
  reader: BufferedReader
) extends TextSource(reader) {

  val TITLE_PREFIX = "Article title: "
  val TITLE_INDEX = TITLE_PREFIX.length
  val ID_INDEX = "Article ID: ".length

  var id = "-1"
  var title = ""

  val sentences = new Iterator[Sentence[Token]] {
    var current = WikiTextSource.this.readLine

    def hasNext: Boolean = current != null
    def next: Sentence[Token] = new Sentence[Token](null) {
      if(current != null) {
        title = current.drop(TITLE_INDEX).trim
        current = WikiTextSource.this.readLine
        id = current.drop(ID_INDEX).trim
        current = WikiTextSource.this.readLine
      }
      val buffer = Buffer(new SimpleToken(current))
      current = WikiTextSource.this.readLine
      while (current != null && !current.trim.startsWith(TITLE_PREFIX)) {
        buffer += new SimpleToken(current)
        current = WikiTextSource.this.readLine
      }

      def tokens: java.util.Iterator[Token] = buffer.toIterator
    }
  }.grouped(1) // assume each document is a whole sentence, since we don't have sentence boundaries

  def hasNext: Boolean = sentences.hasNext

  def next: Document[Token] = {
    new Document[Token](id, title) {
      def iterator: java.util.Iterator[Sentence[Token]] = {
        sentences.next.toIterator
      }
    }
  }
}
