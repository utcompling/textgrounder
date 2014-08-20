///////////////////////////////////////////////////////////////////////////////
//  CoTraining.scala
//
//  Copyright (C) 2014 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder
package geolocate

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Left, Right}
import scala.util.control.Breaks._

import java.io._

import opennlp.fieldspring.tr.app._
import opennlp.fieldspring.tr.resolver._
import opennlp.fieldspring.tr.text._
import opennlp.fieldspring.tr.text.prep._
import opennlp.fieldspring.tr.text.io._
import opennlp.fieldspring.tr.util.TextUtil

import gridlocate._

import util.collection._
import util.error.unsupported
import util.io.localfh
import util.print.{errprint, errout}
import util.spherical._
import util.textdb._

/*
 * How to hook co-training into TextGrounder:
 *
 * We want co-training to be another meta-algorithm like reranking.
 * Reranking hooks into create_ranker() where a "ranker" is a document
 * geolocator that returns a ranking over cells for a given document.
 *
 * The current code in TextGrounder to read in a corpus doesn't read in
 * the raw text. In fact, that isn't normally available in the corpora.
 * We need to do this differently. FieldSpring does have a Corpus class
 * made up of documents, sentences and tokens, where tokens can be toponyms.
 * We might be able to hook into that code to do the reading; or we do
 * it ourselves and then create a parallel FieldSpring corpus from it.
 * We will definitely also need to convert the raw-text corpus into
 * TextGrounder documents (GridDocs). That is done using
 * create_ranker_from_document_streams(), which takes a set of streams
 * in raw format (basically, a row direct from a TextDB database). We will
 * need to fake the row and corresponding schema. This needs to be done
 * within GeolocateDriver because the driver stores various parameters
 * that control operation of the grid/ranker creation.
 */

// abstract class Token {
//   def word: String
// }
// 
// case class Word(word: String) extends Token(word)
// case class Toponym(word: String, candidates: Seq[Candidate],
//   resolved: Option[Candidate]) extends Token(word)
// class Candidate(location: String, coord: Co)
// 
// class Doc(text: IndexedSeq[Token], lm: LangModel, label: Option[SphereCoord],
//   prob: Double) {
// }

class CDoc(val fsdoc: Document[Token], var score: Double) {
  val schema = new Schema(Iterable("title", "coord", "unigram-counts"),
    Map("corpus-type" -> "generic", "split" -> "training"))

  /**
   * Convert the stored FieldSpring-style documents to TextDB rows so they
   * can be used to create a TextGrounder document geolocator (ranker).
   * FIXME: This seems hackish, especially having to convert the words into
   * a serialized string which is then parsed.
   */
  def get_textdb_row = {
    val words = intmap[String]()
    for (sent <- fsdoc; token <- sent.getTokens; form = token.getForm)
      words(form) += 1
    val doc_gold = fsdoc.getGoldCoord
    val coord =
      if (doc_gold == null) "" else
        "%s,%s" format (doc_gold.getLatDegrees, doc_gold.getLngDegrees)
    val props = IndexedSeq(fsdoc.getId, coord, Encoder.count_map(words))
    Row(schema, props)
  }

  def get_textdb_doc_status_row = {
    val row = get_textdb_row
    DocStatus(localfh, "unknown", 1, Some(row), "processed", "", "")
  }

  val doc_counter_tracker =
    new DocCounterTracker[GridDoc[SphereCoord]]("unknown", null)
  /**
   * Convert a FieldSpring-style document into a TextGrounder document.
   */
  def get_grid_doc(ranker: GridRanker[SphereCoord]) = {
    // Convert to a raw document (textdb row)
    val row = get_textdb_doc_status_row
    // Convert to a GridDoc
    val rowdocstat = ranker.grid.docfact.raw_document_to_document_status(
      row, skip_no_coord = false, note_globally = false)
    val docstat = rowdocstat map_all { case (row, griddoc) => griddoc }
    val maybe_griddoc = doc_counter_tracker.handle_status(docstat)
    // raw_document_to_document_status() doesn't compute the backoff
    // stats; we need to do this
    maybe_griddoc.foreach { _.lang_model.finish_after_global() }
    maybe_griddoc
  }

  def debug_print(prefix: String = "") {
    errprint("%sDocument %s [%s pred=%s]:", prefix, fsdoc.getId,
      fsdoc.getGoldCoord, fsdoc.getSystemCoord)
    for (sent <- fsdoc) {
      val words = for (token <- sent; form = token.getForm) yield {
        if (token.isToponym) {
          val toponym = token.asInstanceOf[Toponym]
          if (toponym.getAmbiguity == 0)
            "[%s amb=0]" format form
          else {
            val gold =
              if (toponym.hasGold)
                " %s" format toponym.getGold.getRegion.getCenter
              else
                ""
            val pred =
              if (toponym.hasSelected)
                " pred=%s" format toponym.getSelected.getRegion.getCenter
              else
                ""
            "[%s%s%s]" format (form, gold, pred)
          }
        } else
          form
      }
      errprint("  %s%s", prefix, words mkString " ")
    }
  }
}


/**
 * A corpus that holds documents. We need the following types:
 *
 * 1. A corpus read from a textdb, such as the original Wikipedia corpus.
 *    You can't iterate through the text of this corpus but it has a
 *    document geolocator and document-level coordinates.
 * 2. A corpus read from a FieldSpring corpus, without attached document-level
 *    coordinates. We need to be able to select portions of the total
 *    documents to be iterated through.
 * 3. A corpus read from a FieldSpring corpus, with attached document-level
 *    coordinates.
 * 4. A corpus created from documents built up from individual words, with
 *    attached document-level coordinates.
 *
 * The operations required are:
 *
 * 1. Iterate through the documents of the corpus, multiple times.
 * 2. Remove documents from the corpus, especially corpus type #2/#3, and
 *    add those documents to another corpus.
 * 3. Create documents from individual words and add them to corpus type #4.
 * 4. Generate a document geolocator from at least #1 (which already comes
 *    with one) and #4.
 * 5. Create a corpus for doing toponym resolution (a StoredCorpus) from
 *    corpus types #2/#3.
 *
 * To implement 2 and 3, we read the documents into a StoredCorpus and then
 * record the documents in a LinkedHashSet, so we can add/remove documents
 * while preserving their order. It's necessary to use a StoredCorpus because
 * the documents in a DocumentSource can be iterated through only once, and
 * the words in such a document iterated through only once.
 * preserving their order. We create another implementation of DocumentSource
 * that iterates through these documents, so that we can create a
 * StoredCorpus from it to do toponym resolution on. (FIXME: Is this necessary?
 * Maybe we can create our own implementation of StoredCorpus, since we
 * do store the documents.)
 */
abstract class CCorpus(
) {
  def docs: Iterable[CDoc]
  def to_docgeo_ranker(driver: GeolocateDriver): GridRanker[SphereCoord]
}

class TextGrounderCCorpus(ranker: GridRanker[SphereCoord]) extends CCorpus {
  def docs = unsupported()
  def to_docgeo_ranker(driver: GeolocateDriver) = ranker
}

class FieldSpringCCorpus extends CCorpus {
  val docs = mutable.LinkedHashSet[CDoc]()

  def this(newdocs: Iterable[CDoc]) {
    this()
    docs ++= newdocs
  }

  def to_stored_corpus = {
    val fscorpus = Corpus.createStoredCorpus
    fscorpus.addSource(new CCorpusDocumentSource(this))
    fscorpus.load
    fscorpus
  }

  def filter(fn: CDoc => Boolean) =
    new FieldSpringCCorpus(docs.filter(fn))

  def +=(x: CDoc) {
    docs += x
  }

  def -=(x: CDoc) {
    docs -= x
  }

  def is_empty = docs.isEmpty

  def size = docs.size

  /**
   * Convert the stored FieldSpring-style documents to TextDB rows so they
   * can be used to create a TextGrounder document geolocator (ranker).
   * FIXME: This seems hackish, especially having to convert the words into
   * a serialized string which is then parsed.
   */
  def get_textdb_doc_status_rows: Iterable[DocStatus[Row]] = {
    for (doc <- docs.toSeq) yield
      doc.get_textdb_doc_status_row
  }

  def to_docgeo_ranker(driver: GeolocateDriver) =
    driver.create_ranker_from_document_streams(Iterable(("co-training",
        _ => get_textdb_doc_status_rows.toIterator)))

  def debug_print(prefix: String = "") {
    for ((doc, index) <- docs.zipWithIndex) {
      doc.debug_print("#%s: " format (index + 1))
    }
  }
}

class CCorpusDocumentSource(corpus: CCorpus) extends DocumentSource {
  val iterator = corpus.docs.toSeq.map(_.fsdoc).iterator
  def hasNext = iterator.hasNext
  def next = iterator.next
}

// document geolocator
class DocGeo(val ranker: GridRanker[SphereCoord]) {
  def label_using_cell_evaluator(corpus: FieldSpringCCorpus) {
    val evaluator = ranker.grid.driver.asInstanceOf[GeolocateDriver].
      create_cell_evaluator(ranker)
    // FIXME: We need to call initialize() on the evaluator with all of the
    // GridDocs.
    for (doc <- corpus.docs) {
      doc.get_grid_doc(ranker) foreach { griddoc =>
        val maybe_result = evaluator.evaluate_document(griddoc)
        maybe_result match {
          case Left(errmess) =>
            errprint("Error evaluating document '%s': %s",
              griddoc.title, errmess)
          case Right(result) =>
            doc.fsdoc.setGoldCoord(result.pred_coord.lat,
              result.pred_coord.long)
            // FIXME: Set the score, which means we need to retrieve it one
            // way or other
        }
      }
    }
  }

  def label(corpus: FieldSpringCCorpus) {
    // FIXME: We need to call initialize() on the GridRanker with all of the
    // GridDocs.
    // FIXME: Can the GridRanker handle initialize() being called multiple
    // times? Fix it so it can.
    for (doc <- corpus.docs) {
      doc.get_grid_doc(ranker) foreach { griddoc =>
          // FIXME: This doesn't work with mean shift. Assumes we're always
          // taking the topmost cell.
        val cells_scores = 
          ranker.evaluate(griddoc, None, include_correct = false)
        val (cell, score) = cells_scores.head
        val coord = cell.get_centroid
        doc.fsdoc.setGoldCoord(coord.lat, coord.long)
        doc.score = score
      }
    }
  }
}

//class InterpolatingDocGeo(r1: GridRanker[SphereCoord],
//    r2: GridRanker[SphereCoord], interp_factor: Double) extends DocGeo {
//
//  require(interp_factor >= 0 && interp_factor <= 1)
//
//  def label(corpus: FieldSpringCCorpus) {
//    // FIXME: We need to call initialize() on the GridRanker with all of the
//    // GridDocs.
//    // FIXME: Can the GridRanker handle initialize() being called multiple
//    // times? Fix it so it can.
//    for (doc <- corpus.docs) {
//      // FIXME: Does it matter that we supply r1 here instead of r2,
//      // or instead of creating the document twice, one for each ranker?
//      doc.get_grid_doc(r1) foreach { griddoc =>
//        // We match up the cells by going through the cells in one of the two,
//        // and for each cell's centroid, looking up the best cell in the other
//        // for this centroid.
//        val cells_scores_1 = 
//          r1.return_ranked_cells(griddoc, None, include_correct = false)
//        val cells_scores_2 = 
//          r2.return_ranked_cells(griddoc, None, include_correct = false)
//        val scores_2_map = cells_scores_2.toMap
//        val cells_scores =
//          cells_scores_1.flatMap { case (cell, score) =>
//            val cell2 = r2.grid.find_best_cell_for_coord(cell.get_centroid,
//              create_non_recorded = false)
//            cell2.flatMap { scores_2_map.get(_) } map { score2 =>
//              (cell, score * interp_factor + score2 * (1 - interp_factor))
//            }
//          }.toSeq.sortWith(_._2 > _._2)
//        if (cells_scores.size == 0)
//          errprint("Error evaluating document '%s': Interpolated cell list is empty",
//            griddoc.title)
//        else {
//          // FIXME: This doesn't work with mean shift. Assumes we're always
//          // taking the topmost cell.
//          val (cell, score) = cells_scores.head
//          val coord = cell.get_centroid
//          doc.fsdoc.setGoldCoord(coord.lat, coord.long)
//          doc.score = score
//        }
//      }
//    }
//  }
//}

object DocGeo {
  def train(corpus: CCorpus, driver: GeolocateDriver) =
    new DocGeo(corpus.to_docgeo_ranker(driver))
  /**
   * Interpolate between two document geolocators. We need to hook into
   * return_ranked_cells() in some way and match up the cells, interpolating
   * the scores. Matching up the cells can be done by going through the
   * cells in one of the two, and for each cell's centroid, looking up the
   * best cell in the other for this centroid. The question is whether we
   * should interpolate here or create a special GridRanker that
   * interpolates between two other GridRankers.
   */
  def interpolate(c1: DocGeo, c2: DocGeo, interp_factor: Double) =
    new DocGeo(new InterpolatingGridRanker(c1.ranker, c2.ranker,
      interp_factor))
}

class TopRes(resolver: Resolver) {
  def resolve(corpus: FieldSpringCCorpus) {
    val stored_corpus = corpus.to_stored_corpus
    val ret = resolver.disambiguate(stored_corpus)
    assert(ret == stored_corpus)
  }
}

class CoTrainer {
  def choose_batch(corpus: FieldSpringCCorpus,
      threshold: Double, minsize: Int): FieldSpringCCorpus = {
    val meet_threshold = corpus.docs.filter { _.score >= threshold }
    if (meet_threshold.size >= minsize)
      new FieldSpringCCorpus(meet_threshold)
    else {
      val sorted = corpus.docs.toSeq.sortWith(_.score > _.score)
      // FIXME: Do we want to preserve the order of the documents in `corpus`?
      // If so we need to find the minimum allowed score and filter the
      // docs that way.
      new FieldSpringCCorpus(sorted.take(minsize))
    }
  }

  /**
   * Return true if one of the toponyms in the document has a candidate that
   * is close to the resolved document location.
   */
  def toponym_candidate_near_location(doc: CDoc, threshold: Double
    ): Boolean = {
    val doc_gold = doc.fsdoc.getGoldCoord
    for (sent <- doc.fsdoc) {
      for (toponym <- sent.getToponyms) {
        if (toponym.hasGold) {
          val location = toponym.getGold.getRegion.getCenter
          if (location.distanceInKm(doc_gold) <= threshold)
            return true
        }
      }
    }
    return false
  }

  /**
   * Construct a pseudo-document created from a given toponym and the tokens
   * surrounding that toponym. The location of the document is set to the
   * location of the toponym.
   */
  def make_pseudo_document(id: String, toponym: Toponym, tokens: Iterable[Token]
    ): CDoc = {
    val coord = toponym.getGold.getRegion.getCenter
    val lat = coord.getLatDegrees
    val long = coord.getLngDegrees
    val doc = new GeoTextDocument(id, "unknown", lat, long)
    doc.addSentence(new SimpleSentence("1", tokens.toSeq))
    new CDoc(doc, 0.0)
  }

  var next_id = 0

  /**
   * For each toponym in each document, construct a pseudo-document consisting
   * of the words within `window` words of the toponym, with the location of
   * the pseudo-document set to the toponym's location.
   */
  def get_pseudo_documents_surrounding_toponyms(corpus: FieldSpringCCorpus,
      window: Int, stoplist: Set[String]) = {
    for (doc <- corpus.docs;
         doc_as_array = TextUtil.getDocAsArray(doc.fsdoc);
         (token, tok_index) <- doc_as_array.zipWithIndex;
         if token.isToponym;
         toponym = token.asInstanceOf[Toponym]
         if toponym.getAmbiguity > 0 && toponym.hasGold) yield {
      next_id += 1
      val start_index = math.max(0, tok_index - window)
      val end_index = math.min(doc_as_array.size, tok_index + window + 1)
      val doc_tokens = doc_as_array.slice(start_index, end_index).
        filterNot(tok => stoplist(tok.getForm))
      make_pseudo_document("%s-%s" format (doc.fsdoc.getId, next_id),
        toponym, doc_tokens)
    }
  }

  /**
   * Do co-training, given a base labeled corpus (e.g. Wikipedia) and
   * an unlabeled corpus.
   *
   * We have four corpora here:
   *
   * 1. A corpus whose source is TextGrounder. This should wrap a ranker,
   *    meaning we can't iterate over the documents.
   * 2. 
   */
  def train(base: GridRanker[SphereCoord], unlabeled: FieldSpringCCorpus,
      resolver: Resolver): (DocGeo, CCorpus) = {
    val driver = base.grid.driver.asInstanceOf[GeolocateDriver]
    // set of labeled documents, originally empty
    var labeled = new FieldSpringCCorpus()
    // set of labeled pseudo-documents corresponding to labeled documents
    var labeled_pseudo = new FieldSpringCCorpus()
    // toponym resolver, possibly trained on wp (if WISTR)
    var topres = new TopRes(resolver)
    val wp_docgeo = new DocGeo(base)
    var iteration = 0
    var docgeo: DocGeo = null
    breakable {
      while (true) {
        iteration += 1
        errprint("Iteration %s: Size of unlabeled corpus: %s docs",
          iteration, unlabeled.size)
        errprint("Iteration %s: Size of labeled corpus: %s docs",
          iteration, labeled.size)
        errprint("Iteration %s: Size of labeled-pseudo corpus: %s docs",
          iteration, labeled_pseudo.size)
        val docgeo1 = DocGeo.train(labeled_pseudo, driver)
        docgeo = DocGeo.interpolate(wp_docgeo, docgeo1,
          driver.params.co_train_interpolate_factor)
        docgeo.label(unlabeled)
        val chosen_labeled = choose_batch(unlabeled,
          driver.params.co_train_min_score, driver.params.co_train_min_size)
        errprint("Iteration %s: Size of chosen unlabeled: %s docs",
          iteration, chosen_labeled.size)
        val accepted_labeled =
          chosen_labeled.filter(doc => toponym_candidate_near_location(
            doc, driver.params.co_train_max_distance))
        errprint("Iteration %s: Size of accepted labeled: %s docs",
          iteration, accepted_labeled.size)
        if (accepted_labeled.is_empty) {
          errprint("Terminating, accepted-labeled corpus is empty")
          break
        }
        errprint("Accepted-labeled corpus:")
        accepted_labeled.debug_print()
        topres.resolve(accepted_labeled)
        val resolved_pseudo =
          get_pseudo_documents_surrounding_toponyms(accepted_labeled,
            driver.params.co_train_window, driver.the_stopwords)
        errprint("Iteration %s: Size of resolved-pseudo corpus: %s docs",
          iteration, resolved_pseudo.size)
        errprint("Resolved-pseudo corpus:")
        for ((doc, index) <- resolved_pseudo.zipWithIndex)
          doc.debug_print("#%s: " format (index + 1))
        for (doc <- resolved_pseudo)
          labeled_pseudo += doc
        for (doc <- accepted_labeled.docs) {
          labeled += doc
          unlabeled -= doc
        }
        if (unlabeled.is_empty) {
          errprint("Terminating, unlabeled corpus is empty")
          break
        }
      }
    }
    (docgeo, labeled)
  }

  def create_resolver(driver: GeolocateDriver): Resolver = {
    val params = driver.params
    params.topres_resolver match {
      case "random" => new RandomResolver
      case "population" => new PopulationResolver
      case "spider" => new WeightedMinDistResolver(params.topres_iterations,
        params.topres_weights_file, params.topres_log_file)
      case "maxent" => new MaxentResolver(params.topres_log_file,
        params.topres_maxent_model_dir)
      case "prob" => new ProbabilisticResolver(params.topres_log_file,
        params.topres_maxent_model_dir, params.topres_write_weights_file,
        params.topres_pop_component, params.topres_dg,
        params.topres_me)
    }
  }

  def read_fieldspring_gold_corpus(corpus: String, corpus_format: String) = {
    val tokenizer = new OpenNLPTokenizer()
    // val recognizer = new OpenNLPRecognizer()

    val gold_corpus = Corpus.createStoredCorpus
    corpus_format match {
      case "trconll" => {
        errout("Reading gold corpus from $corpus ...")
        val gold_file = new File(corpus)
        if(gold_file.isDirectory)
            gold_corpus.addSource(new TrXMLDirSource(gold_file, tokenizer))
        else
            gold_corpus.addSource(new TrXMLSource(new BufferedReader(new FileReader(gold_file)), tokenizer))
        gold_corpus.setFormat(BaseApp.CORPUS_FORMAT.TRCONLL)
        gold_corpus.load()
        errprint("done.")
      }
    }

    val ccorp = new FieldSpringCCorpus
    for (doc <- gold_corpus)
      ccorp += new CDoc(doc.asInstanceOf[Document[Token]], 0.0)
    ccorp
  }
}
