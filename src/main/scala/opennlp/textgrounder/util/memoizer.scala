///////////////////////////////////////////////////////////////////////////////
//  Memoizer.scala
//
//  Copyright (C) 2011-2013 Ben Wing, The University of Texas at Austin
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
package util
package memoizer

import scala.collection.mutable

import com.codahale.trove.{mutable => trovescala}

import collection._
import print.errprint

// We comment this out and don't use it to avoid the possibility that
// accesses to Trove hash tables go through the boxing/unboxing interface
// to/from mutable.Map[]. (FIXME: Is this actually an issue?)
//
//trait HashTableFactory {
//  /**
//   * Create a mutable map from Ints to Ints, with 0 as default value.
//   * (I.e., attempting to fetch a nonexistent key will yield 0 rather than
//   * throw an error.)
//   */
//  def create_int_int_map: mutable.Map[Int, Int]
//  /**
//   * Create a mutable map from Ints to Doubles, with 0.0 as default value.
//   * (I.e., attempting to fetch a nonexistent key will yield 0.0 rather than
//   * throw an error.)
//   */
//  def create_int_double_map: mutable.Map[Int, Double]
//  /**
//   * Create a mutable map from Ints to arbitrary reference type T.
//   */
//  def create_int_object_map[T]: mutable.Map[Int, T]
//  /**
//   * Create a mutable map from arbitrary reference type T to Ints, with 0
//   * as default value. (I.e., attempting to fetch a nonexistent key will
//   * yield 0 rather than throw an error.)
//   */
//  def create_object_int_map[T]: mutable.Map[T, Int]
//}

object ScalaHashTableFactory /* extends HashTableFactory */ {
  def create_int_int_map = intmap[Int]()
  def create_int_double_map = doublemap[Int]()
  def create_int_object_map[T] = mutable.Map[Int,T]()
  def create_object_int_map[T] = intmap[T]()
}

/*
 * Use Trove for extremely fast and memory-efficient hash tables, making use of
 * the Trove-Scala interface for easy access to the Trove hash tables.
 */
object TroveHashTableFactory /* extends HashTableFactory */ {
  def create_int_int_map = trovescala.IntIntMap()
  def create_int_double_map = trovescala.IntDoubleMap()
  def create_int_object_map[T] = trovescala.IntObjectMap[T]()
  def create_object_int_map[T] = trovescala.ObjectIntMap[T]()
}

/**
 * A class for "memoizing" values, i.e. mapping them to some other type
 * (e.g. Int) that should be faster to compare and potentially require
 * less space.
 *
 * @tparam T Type of unmemoized value.
 * @tparam U Type of memoized value.
 */
trait Memoizer[T,U] {
  /**
   * Map a value to its memoized form.
   */
  def to_index(value: T): U
  /**
   * Map a value to its memoized form but only if it has already been seen.
   */
  def to_index_if(value: T): Option[U]
  /**
   * Map a value out of its memoized form.
   */
  def to_string(value: U): T
}

/**
 * Standard memoizer for mapping values to Ints. Specialization of
 * `Memoizer` for Ints, without boxing or unboxing. Uses
 * TroveHashTableFactory for efficiency. Lowest index returned is always
 * 0, so that indices can be directly used in an array.
 */
trait ToIntMemoizer[T] {
  // Use Trove for fast, efficient hash tables.
  val hashfact = TroveHashTableFactory
  // Alternatively, just use the normal Scala hash tables.
  // val hashfact = ScalaHashTableFactory

  /* The raw indices used in the hash table aren't the same as the
   * external indices because TroveHashTableFactory by default uses 0
   * to indicate that an item wasn't found in an x->int map. So we
   * add 1 to the external index to get the raw index.
   *
   * FIXME: Can we set the not-found item differently, e.g. -1?
   * (Yes but only at object-creation time, and we need to modify
   * trove-scala to allow it to be set, and figure out how to retrieve
   * the DEFAULT_CAPACITY and DEFAULT_LOAD_FACTOR values from Trove,
   * because they must be specified if we are to set the not-found item.)
   */
  type RawIndex = Int
  // Don't set minimum_index to 0. I think this causes problems because
  // TroveHashTableFactory by default returns 0 when an item isn't found
  // in an x->int map.
  // Smallest index returned.
  protected val minimum_raw_index: RawIndex = 1
  protected var next_raw_index: RawIndex = minimum_raw_index
  def number_of_indices = next_raw_index - minimum_raw_index
  def maximum_index = number_of_indices - 1

  // For replacing items with ints.  This should save space on 64-bit
  // machines (object pointers are 8 bytes, ints are 4 bytes) and might
  // also speed lookup.
  protected val value_id_map = hashfact.create_object_int_map[T]

  // Map in the opposite direction.
  protected val id_value_map = hashfact.create_int_object_map[T]

  def to_index_if(value: T) = synchronized {
    value_id_map.get(value).map(_ - minimum_raw_index)
  }

  def to_index(value: T) = synchronized {
    val lookup = to_index_if(value)
    // println("Saw value=%s, index=%s" format (value, lookup))
    lookup match {
      case Some(index) => index
      case None => {
        val newind = next_raw_index
        next_raw_index += 1
        value_id_map(value) = newind
        id_value_map(newind) = value
        newind - minimum_raw_index
      }
    }
  }

  def to_string(index: Int) = synchronized {
    id_value_map(index + minimum_raw_index)
  }
}

// Doesn't currently work because overriding this way leads to error
// "value value_id_map has incompatible type".
//trait IntIntMemoizer extends ToIntMemoizer[Int] {
//  protected override val value_id_map = hashfact.create_int_int_map
//
//  // Map in the opposite direction.
//  protected override val id_value_map = hashfact.create_int_int_map
//}

/**
 * Version for debugging the String-to-Int memoizer.
 */
trait TestStringIntMemoizer extends ToIntMemoizer[String] {
  override def to_index(value: String) = {
    // if (debug("memoize")) {
    val retval =
      to_index_if(value) match {
        case Some(index) => {
          errprint("Memoizing existing string %s to ID %s", value, index)
          index
        }
        case None => {
          val index = super.to_index(value)
          errprint("Memoizing new string %s to ID %s", value, index)
          index
        }
      }
    assert(super.to_string(retval) == value)
    retval
  }

  override def to_string(value: Int) = {
    if (!(id_value_map contains value)) {
      errprint("Can't find ID %s in id_value_map", value)
      errprint("Word map:")
      var its = id_value_map.toList.sorted
      for ((key, value) <- its)
        errprint("%s = %s", key, value)
      assert(false, "Exiting due to bad code in unmemoize")
      null
    } else {
      val string = super.to_string(value)

      // if (debug("memoize"))
        errprint("Unmemoizing existing ID %s to string %s", value, string)

      assert(super.to_index(string) == value)
      string
    }
  }
}

/**
 * A memoizer for testing that doesn't actually do anything -- the memoized
 * values are the same as the unmemoized values.
 */
trait IdentityMemoizer[T] extends Memoizer[T,T] {
  def to_index(value: T) = value
  def to_index_if(value: T) = Some(value)
  def to_string(value: T) = value
}

