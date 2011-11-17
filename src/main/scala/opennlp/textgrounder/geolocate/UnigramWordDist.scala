///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.geolocate

import math._
import collection.mutable
import util.control.Breaks._

import java.io._

import opennlp.textgrounder.util.collectionutil.DynamicArray
import opennlp.textgrounder.util.ioutil.{errprint, warning, FileHandler}
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.osutil.output_resource_usage

import GeolocateDriver.Params
import GeolocateDriver.Debug._
import WordDist.memoizer._

/**
 * Unigram word distribution with a table listing counts for each word,
 * initialized from the given key/value pairs.
 *
 * @param key Array holding keys, possibly over-sized, so that the internal
 *   arrays from DynamicArray objects can be used
 * @param values Array holding values corresponding to each key, possibly
 *   oversize
 * @param num_words Number of actual key/value pairs to be stored 
 *   statistics.
 */

abstract class UnigramWordDist(
  keys: Array[Word],
  values: Array[Int],
  num_words: Int
) extends WordDist {
  /** A map (or possibly a "sorted list" of tuples, to save memory?) of
      (word, count) items, specifying the counts of all words seen
      at least once.
   */
  val counts = create_word_int_map()
  for (i <- 0 until num_words)
    counts(keys(i)) = values(i)
  var num_word_tokens = counts.values.sum
  
  def num_word_types = counts.size

  def innerToString: String

  override def toString = {
    val finished_str =
      if (!finished) ", unfinished" else ""
    val num_words_to_print = 15
    val need_dots = counts.size > num_words_to_print
    val items =
      for ((word, count) <- counts.view(0, num_words_to_print))
      yield "%s=%s" format (unmemoize_word(word), count) 
    val words = (items mkString " ") + (if (need_dots) " ..." else "")
    "WordDist(%d tokens%s%s, %s)" format (
        num_word_tokens, innerToString, finished_str, words)
  }

  def add_document(words: Traversable[String], ignore_case: Boolean=true,
      stopwords: Set[String]=Set[String]()) {
    assert(!finished)
    for {word <- words
         val wlower = if (ignore_case) word.toLowerCase() else word
         if !stopwords(wlower) } {
      counts(memoize_word(wlower)) += 1
      num_word_tokens += 1
    }
  }

  def add_word_distribution(xworddist: WordDist) {
    assert(!finished)
    val worddist = xworddist.asInstanceOf[UnigramWordDist]
    for ((word, count) <- worddist.counts)
      counts(word) += count
    num_word_tokens += worddist.num_word_tokens
  }

  def finish_before_global(minimum_word_count: Int = 0) {
    // make sure counts not null (eg document in coords file but not counts file)
    if (counts == null || finished) return

    // If 'minimum_word_count' was given, then eliminate words whose count
    // is too small.
    if (minimum_word_count > 1) {
      for ((word, count) <- counts if count < minimum_word_count) {
        num_word_tokens -= count
        counts -= word
      }
    }
  }

  /**
   * This is a basic unigram implementation of the computation of the
   * KL-divergence between this distribution and another distribution.
   * Useful for checking against other, faster implementations.
   * 
   * Computing the KL divergence is a bit tricky, especially in the
   * presence of smoothing, which assigns probabilities even to words not
   * seen in either distribution.  We have to take into account:
   * 
   * 1. Words in this distribution (may or may not be in the other).
   * 2. Words in the other distribution that are not in this one.
   * 3. Words in neither distribution but seen globally.
   * 4. Words never seen at all.
   * 
   * The computation of steps 3 and 4 depends heavily on the particular
   * smoothing algorithm; in the absence of smoothing, these steps
   * contribute nothing to the overall KL-divergence.
   * 
   * @param xother The other distribution to compute against.
   * @param partial If true, only do step 1 above.
   * @param return_contributing_words If true, return a map listing
   *   the words in both distributions and the amount of total
   *   KL-divergence they compute, useful for debugging.
   *   
   * @returns A tuple of (divergence, word_contribs) where the first
   *   value is the actual KL-divergence and the second is the map
   *   of word contributions as described above; will be null if
   *   not requested.
   */
  def slow_kl_divergence_debug(xother: WordDist, partial: Boolean=false,
      return_contributing_words: Boolean=false) = {
    val other = xother.asInstanceOf[UnigramWordDist]
    assert(finished)
    assert(other.finished)
    var kldiv = 0.0
    val contribs =
      if (return_contributing_words) mutable.Map[Word, Double]() else null
    // 1.
    for (word <- counts.keys) {
      val p = lookup_word(word)
      val q = other.lookup_word(word)
      if (p <= 0.0 || q <= 0.0)
        errprint("Warning: problematic values: p=%s, q=%s, word=%s", p, q, word)
      else {
        kldiv += p*(log(p) - log(q))
        if (return_contributing_words)
          contribs(word) = p*(log(p) - log(q))
      }
    }

    if (partial)
      (kldiv, contribs)
    else {
      // Step 2.
      for (word <- other.counts.keys if !(counts contains word)) {
        val p = lookup_word(word)
        val q = other.lookup_word(word)
        kldiv += p*(log(p) - log(q))
        if (return_contributing_words)
          contribs(word) = p*(log(p) - log(q))
      }

      val retval = kldiv + kl_divergence_34(other)
      (retval, contribs)
    }
  }

  /**
   * Steps 3 and 4 of KL-divergence computation.
   * @seealso #slow_kl_divergence_debug
   */
  def kl_divergence_34(other: UnigramWordDist): Double
  
  def get_nbayes_logprob(xworddist: WordDist) = {
    val worddist = xworddist.asInstanceOf[UnigramWordDist]
    var logprob = 0.0
    for ((word, count) <- worddist.counts) {
      val value = lookup_word(word)
      if (value <= 0) {
        // FIXME: Need to figure out why this happens (perhaps the word was
        // never seen anywhere in the training data? But I thought we have
        // a case to handle that) and what to do instead.
        errprint("Warning! For word %s, prob %s out of range", word, value)
      } else
        logprob += log(value)
    }
    // FIXME: Also use baseline (prior probability)
    logprob
  }

  def find_most_common_word(pred: String => Boolean) = {
    val filtered =
      (for ((word, count) <- counts if pred(unmemoize_word(word)))
        yield (word, count)).toSeq
    if (filtered.length == 0) None
    else {
      val (maxword, maxcount) = filtered maxBy (_._2)
      Some(maxword)
    }
  }
}  

/**
 * General factory for UnigramWordDist distributions.
 */ 
abstract class UnigramWordDistFactory extends WordDistFactory {
  def create_populated_word_dist(keys: Array[Word], values: Array[Int],
    num_words: Int, note_globally: Boolean): UnigramWordDist

  def read_word_counts(table: DistDocumentTable,
      filehand: FileHandler, filename: String, stopwords: Set[String]) {
    val initial_dynarr_size = 1000
    val keys_dynarr =
      new DynamicArray[Word](initial_alloc = initial_dynarr_size)
    val values_dynarr =
      new DynamicArray[Int](initial_alloc = initial_dynarr_size)

    // This is basically a one-off debug statement because of the fact that
    // the experiments published in the paper used a word-count file generated
    // using an older algorithm for determining the geolocated coordinate of
    // a document.  We didn't record the corresponding document-data
    // file, so we need a way of regenerating it using the intersection of
    // documents in the document-data file we actually used for the experiments
    // and the word-count file we used.
    var stream: PrintStream = null
    var writer: GeoDocumentWriter = null
    if (debug("wordcountdocs")) {
      // Change this if you want a different file name
      val wordcountdocs_filename = "wordcountdocs-combined-document-data.txt"
      stream = filehand.openw(wordcountdocs_filename)
      // See write_document_file() in GeoDocument.scala
      writer =
        new GeoDocumentWriter(stream, GeoDocumentData.combined_document_data_outfields)
      writer.output_header()
    }

    var num_word_tokens = 0
    var title = null: String

    def one_document_probs() {
      if (num_word_tokens == 0) return
      val doc = table.lookup_document(title)
      if (doc == null) {
        warning("Skipping document %s, not in table", title)
        table.num_documents_with_word_counts_but_not_in_table += 1
        return
      }
      if (debug("wordcountdocs"))
        writer.output_row(doc)
      table.num_word_count_documents_by_split(doc.split) += 1
      // If we are evaluating on the dev set, skip the test set and vice
      // versa, to save memory and avoid contaminating the results.
      if (doc.split != "training" && doc.split != Params.eval_set)
        return
      // Don't train on test set
      doc.dist = create_populated_word_dist(keys_dynarr.array,
          values_dynarr.array, keys_dynarr.length,
          note_globally = (doc.split == "training"))
    }

    val task = new MeteredTask("document", "reading distributions of")
    errprint("Reading word counts from %s...", filename)
    errprint("")

    // Written this way because there's another line after the for loop,
    // corresponding to the else clause of the Python for loop
    breakable {
      for (line <- filehand.openr(filename)) {
        if (line.startsWith("Article title: ")) {
          if (title != null)
            one_document_probs()
          // Stop if we've reached the maximum
          if (task.item_processed(maxtime = Params.max_time_per_stage))
            break
          if ((Params.num_training_docs > 0 &&
            task.num_processed >= Params.num_training_docs)) {
            errprint("")
            errprint("Stopping because limit of %s documents reached",
              Params.num_training_docs)
            break
          }

          // Extract title and set it
          val titlere = "Article title: (.*)$".r
          line match {
            case titlere(ti) => title = ti
            case _ => assert(false)
          }
          keys_dynarr.clear()
          values_dynarr.clear()
          num_word_tokens = 0
        } else if (line.startsWith("Article coordinates) ") ||
          line.startsWith("Article ID: "))
          ()
        else {
          val linere = "(.*) = ([0-9]+)$".r
          line match {
            case linere(xword, xcount) => {
              var word = xword
              if (!Params.preserve_case_words) word = word.toLowerCase
              val count = xcount.toInt
              if (!(stopwords contains word) ||
                Params.include_stopwords_in_document_dists) {
                num_word_tokens += count
                keys_dynarr += memoize_word(word)
                values_dynarr += count
              }
            }
            case _ =>
              warning("Strange line, can't parse: title=%s: line=%s",
                title, line)
          }
        }
      }
      one_document_probs()
    }

    if (debug("wordcountdocs"))
      stream.close()
    task.finish()
    table.num_documents_with_word_counts += task.num_processed
    output_resource_usage()
  }
}

