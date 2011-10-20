package opennlp.textgrounder.geolocate

import NlpUtil._
import Debug._
import WordDist.memoizer._
import WordDist.SmoothedWordDist

import math._
import collection.mutable
import com.codahale.trove.mutable._

// val use_sorted_list = false

//////////////////////////////////////////////////////////////////////////////
//                             Word distributions                           //
//////////////////////////////////////////////////////////////////////////////

object IntStringMemoizer {
  type Word = Int
  val invalid_word: Word = 0

  protected var next_word_count: Word = 1

  // For replacing strings with ints.  This should save space on 64-bit
  // machines (string pointers are 8 bytes, ints are 4 bytes) and might
  // also speed lookup.
  protected val word_id_map = mutable.Map[String,Word]()

  // Map in the opposite direction.
  protected val id_word_map = mutable.Map[Word,String]()

  def memoize_word(word: String) = {
    val index = word_id_map.getOrElse(word, 0)
    if (index != 0) index
    else {
      val newind = next_word_count
      next_word_count += 1
      word_id_map(word) = newind
      id_word_map(index) = word
      newind
    }
  }

  def unmemoize_word(word: Word) = id_word_map(word)

  def create_word_int_map() = IntIntMap()
  type WordIntMap = IntIntMap
  def create_word_double_map() = IntDoubleMap()
  type WordDoubleMap = IntDoubleMap
}

object IdentityMemoizer {
  type Word = String
  val invalid_word: Word = null
  def memoize_word(word: String): Word = word
  def unmemoize_word(word: Word): String = word

  def create_word_int_map() = intmap[Word]()
  def create_word_double_map() = doublemap[Word]()
}

object TrivialIntMemoizer {
  type Word = Int
  val invalid_word: Word = 0
  def memoize_word(word: String): Word = 1
  def unmemoize_word(word: Word): String = "foo"

  def create_word_int_map() = IntStringMemoizer.create_word_int_map()
  def create_word_double_map() = IntStringMemoizer.create_word_double_map()
}

object WordDist {
  val memoizer = IntStringMemoizer
  type SmoothedWordDist = PseudoGoodTuringSmoothedWordDist
  val SmoothedWordDist = PseudoGoodTuringSmoothedWordDist

  // Total number of word types seen (size of vocabulary)
  var num_word_types = 0

  // Total number of word tokens seen
  var num_word_tokens = 0

  def apply(keys: Array[Word], values: Array[Int], num_words: Int,
            note_globally: Boolean) =
    new SmoothedWordDist(keys, values, num_words, note_globally)

  def apply():SmoothedWordDist =
    apply(Array[Word](), Array[Int](), 0, note_globally=false)
}

/**
 * Create a word distribution given a table listing counts for each word,
 * initialized from the given key/value pairs.
 *
 * @param key Array holding keys, possibly over-sized, so that the internal
 *   arrays from DynamicArray objects can be used
 * @param values Array holding values corresponding to each key, possibly
 *   oversize
 * @param num_words Number of actual key/value pairs to be stored 
 *   statistics.
 */

abstract class WordDist(
  keys: Array[Word],
  values: Array[Int],
  num_words: Int
) {
  /** A map (or possibly a "sorted list" of tuples, to save memory?) of
      (word, count) items, specifying the counts of all words seen
      at least once.
   */
  val counts = create_word_int_map()
  for (i <- 0 until num_words)
    counts(keys(i)) = values(i)
  /** Total number of word tokens seen */
  var total_tokens = counts.values.sum

  /** Whether we have finished computing the distribution in 'counts'. */
  var finished = false

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
        total_tokens, innerToString, finished_str, words)
  }

  /**
   * Incorporate a list of words into the distribution.
   */
  def add_words(words: Traversable[String], ignore_case: Boolean=true,
      stopwords: Set[String]=Set[String]()) {
    assert(!finished)
    for {word <- words
         val wlower = if (ignore_case) word.toLowerCase() else word
         if !stopwords(wlower) } {
      counts(memoize_word(wlower)) += 1
      total_tokens += 1
    }
  }

  /**
   * Incorporate counts from the given distribution into our distribution.
   */
  def add_word_distribution(worddist: WordDist) {
    assert (!finished)
    for ((word, count) <- worddist.counts)
      counts(word) += count
    total_tokens += worddist.total_tokens
  }

  /**
   * Finish computation of the word distribution.  This must be called AFTER
   * finish_global_distribution(), because of the computation below of
   * overall_unseen_mass, which depends on the global overall_word_probs.
   */
  def finish(minimum_word_count: Int = 0) {

    // make sure counts not null (eg article in coords file but not counts file)
    if (counts == null || finished) return

    // If 'minimum_word_count' was given, then eliminate words whose count
    // is too small.
    if (minimum_word_count > 1)
      for ((word, count) <- counts if count < minimum_word_count) {
        total_tokens -= count
        counts -= word
      }

    finish_after_global()
  }

  def finish_after_global()

  /**
   * Check fast and slow versions against each other.
   */
  def test_kl_divergence(other: WordDist, partial: Boolean=false) = {
    assert(finished)
    assert(other.finished)
    val fast_kldiv = fast_kl_divergence(other, partial)
    val slow_kldiv = slow_kl_divergence(other, partial)
    if (abs(fast_kldiv - slow_kldiv) > 1e-8) {
      errprint("Fast KL-div=%s but slow KL-div=%s", fast_kldiv, slow_kldiv)
      assert(fast_kldiv == slow_kldiv)
    }
    fast_kldiv
  }

  def slow_kl_divergence_debug(other: WordDist, partial: Boolean=false,
      return_contributing_words: Boolean=false):
    (Double, collection.Map[Word, Double])

  def slow_kl_divergence(other: WordDist, partial: Boolean=false) = {
    val (kldiv, contribs) = slow_kl_divergence_debug(other, partial, false)
    kldiv
  }

  def fast_kl_divergence(other: WordDist, partial: Boolean=false): Double

  def fast_cosine_similarity(other: WordDist, partial: Boolean=false): Double

  def fast_smoothed_cosine_similarity(other: WordDist, partial: Boolean=false): Double

  def symmetric_kldiv(other: WordDist, partial: Boolean=false) = {
    0.5*this.fast_kl_divergence(other, partial) +
    0.5*this.fast_kl_divergence(other, partial)
  }

  def lookup_word(word: Word): Double
  
  def find_most_common_word(pred: String => Boolean) = {
    // Look for the most common word matching a given predicate.
    // Predicate is passed the raw (unmemoized) form of a word.
    // But there may not be any.  max() will raise an error if given an
    // empty sequence, so insert a bogus value into the sequence with a
    // negative count.
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

