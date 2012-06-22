///////////////////////////////////////////////////////////////////////////////
//  Memoizer.scala
//
//  Copyright (C) 2011, 2012 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.worddist

import collection.mutable

import com.codahale.trove.{mutable => trovescala}

import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.printutil.errprint

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
 * The memoizer we actually use.  Maps word strings to Ints.  Uses Trove
 * for extremely fast and memory-efficient hash tables, making use of the
 * Trove-Scala interface for easy access to the Trove hash tables.
 */
class IntStringMemoizer extends Memoizer {
  type Word = Int
  val invalid_word: Word = 0

  protected var next_word_count: Word = 1

  // For replacing strings with ints.  This should save space on 64-bit
  // machines (string pointers are 8 bytes, ints are 4 bytes) and might
  // also speed lookup.
  //protected val word_id_map = mutable.Map[String,Word]()
  protected val word_id_map = trovescala.ObjectIntMap[String]()

  // Map in the opposite direction.
  //protected val id_word_map = mutable.Map[Word,String]()
  protected val id_word_map = trovescala.IntObjectMap[String]()

  def memoize_string(word: String) = {
    val index = word_id_map.getOrElse(word, 0)
    if (index != 0) index
    else {
      val newind = next_word_count
      next_word_count += 1
      word_id_map(word) = newind
      id_word_map(newind) = word
      newind
    }
  }

  def unmemoize_string(word: Word) = id_word_map(word)

  def create_word_int_map() = trovescala.IntIntMap()
  type WordIntMap = trovescala.IntIntMap
  def create_word_double_map() = trovescala.IntDoubleMap()
  type WordDoubleMap = trovescala.IntDoubleMap
}

/**
 * The memoizer we actually use.  Maps word strings to Ints.  Uses Trove
 * for extremely fast and memory-efficient hash tables, making use of the
 * Trove-Scala interface for easy access to the Trove hash tables.
 */
class TestIntStringMemoizer extends IntStringMemoizer {
  override def memoize_string(word: String) = {
    val cur_nwi = next_word_count
    val index = super.memoize_string(word)
    if (debug("memoize")) {
      if (next_word_count != cur_nwi)
        errprint("Memoizing new string %s to ID %s", word, index)
      else
        errprint("Memoizing existing string %s to ID %s", word, index)
    }
    assert(super.unmemoize_string(index) == word)
    index
  }

  override def unmemoize_string(word: Word) = {
    if (!(id_word_map contains word)) {
      errprint("Can't find ID %s in id_word_map", word)
      errprint("Word map:")
      var its = id_word_map.toList.sorted
      for ((key, value) <- its)
        errprint("%s = %s", key, value)
      assert(false, "Exiting due to bad code in unmemoize_string")
      null
    } else {
      val string = super.unmemoize_string(word)
      if (debug("memoize"))
        errprint("Unmemoizing existing ID %s to string %s", word, string)
      assert(super.memoize_string(string) == word)
      string
    }
  }
}

/**
 * A memoizer for testing that doesn't actually do anything -- the memoized
 * words are also strings.  This tests that we don't make any assumptions
 * about memoized words being Ints.
 */
object IdentityMemoizer extends Memoizer {
  type Word = String
  val invalid_word: Word = null
  def memoize_string(word: String): Word = word
  def unmemoize_string(word: Word): String = word

  type WordIntMap = mutable.Map[Word, Int]
  def create_word_int_map() = intmap[Word]()
  type WordDoubleMap = mutable.Map[Word, Double]
  def create_word_double_map() = doublemap[Word]()
}

