package opennlp.scalabha.classify.perceptron

import collection.mutable

/**
 * A class for "memoizing" words, i.e. mapping them to some other type
 * (e.g. Int) that should be faster to compare and potentially require
 * less space.
 */
abstract class Memoizer {
  /**
   * The type of a memoized word.
   */
  type Word
  /**
   * Map a word as a string to its memoized form.
   */
  def memoize_string(word: String): Word
  /**
   * Map a word from its memoized form back to a string.
   */
  def unmemoize_string(word: Word): String

  /**
   * The type of a mutable map from memoized words to Ints.
   */
  type WordIntMap
  /**
   * Create a mutable map from memoized words to Ints.
   */
  def create_word_int_map(): WordIntMap
  /**
   * The type of a mutable map from memoized words to Doubles.
   */
  type WordDoubleMap
  /**
   * Create a mutable map from memoized words to Doubles.
   */
  def create_word_double_map(): WordDoubleMap

  lazy val blank_memoized_string = memoize_string("")

  def lowercase_memoized_word(word: Word) =
    memoize_string(unmemoize_string(word).toLowerCase)
}

/**
 * The memoizer we actually use.  Maps word strings to Ints.
 *
 * @param minimum_index Minimum index used, usually either 0 or 1.
 */
class IntStringMemoizer(val minimum_index: Int = 0) extends Memoizer {
  type Word = Int

  protected var next_word_count: Word = minimum_index

  def number_of_entries = next_word_count - minimum_index

  // For replacing strings with ints.  This should save space on 64-bit
  // machines (string pointers are 8 bytes, ints are 4 bytes) and might
  // also speed lookup.
  protected val word_id_map = mutable.Map[String,Word]()
  //protected val word_id_map = trovescala.ObjectIntMap[String]()

  // Map in the opposite direction.
  protected val id_word_map = mutable.Map[Word,String]()
  //protected val id_word_map = trovescala.IntObjectMap[String]()

  def memoize_string(word: String) = {
    val index = word_id_map.getOrElse(word, -1)
    // println("Saw word=%s, index=%s" format (word, index))
    if (index != -1) index
    else {
      val newind = next_word_count
      next_word_count += 1
      word_id_map(word) = newind
      id_word_map(newind) = word
      newind
    }
  }

  def unmemoize_string(word: Word) = id_word_map(word)

  //def create_word_int_map() = trovescala.IntIntMap()
  //type WordIntMap = trovescala.IntIntMap
  //def create_word_double_map() = trovescala.IntDoubleMap()
  //type WordDoubleMap = trovescala.IntDoubleMap
  def create_word_int_map() = mutable.Map[Word,Int]()
  type WordIntMap = mutable.Map[Word,Int]
  def create_word_double_map() = mutable.Map[Word,Double]()
  type WordDoubleMap = mutable.Map[Word,Double]
}

// /**
//  * Version that uses Trove for extremely fast and memory-efficient hash
//  * tables, making use of the Trove-Scala interface for easy access to the
//  * Trove hash tables.
//  */
// class TroveIntStringMemoizer(
//   minimum_index: Int = 0
// ) extends IntStringMemoizer(minimum_index) {
//   override protected val word_id_map = trovescala.ObjectIntMap[String]()
//   override protected val id_word_map = trovescala.IntObjectMap[String]()
//   override def create_word_int_map() = trovescala.IntIntMap()
//   override type WordIntMap = trovescala.IntIntMap
//   override def create_word_double_map() = trovescala.IntDoubleMap()
//   override type WordDoubleMap = trovescala.IntDoubleMap
// }
