///////////////////////////////////////////////////////////////////////////////
//  memoizer.scala
//
//  Copyright (C) 2011-2014 Ben Wing, The University of Texas at Austin
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
import error._
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
  def create_int_long_map = longmap[Int]()
  def create_long_int_map = intmap[Long]()
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
  def create_int_long_map = trovescala.IntLongMap()
  def create_long_int_map = trovescala.LongIntMap()
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
    assert_==(super.to_raw(retval), value)
    retval
  }

  override def to_raw(value: Int) = {
    if (!(id_value_map contains value)) {
      errprint("Can't find ID %s in id_value_map", value)
      errprint("Word map:")
      var its = id_value_map.toList.sorted
      for ((key, value) <- its)
        errprint("%s = %s", key, value)
      assert(false, "Exiting due to bad code in unmemoize")
      null
    } else {
      val string = super.to_raw(value)

      // if (debug("memoize"))
        errprint("Unmemoizing existing ID %s to string %s", value, string)

      assert_==(super.to_index(string), value)
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

