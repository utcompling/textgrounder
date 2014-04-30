///////////////////////////////////////////////////////////////////////////////
//  TermUserModel.scala
//
//  Copyright (C) 2013-2014 Ben Wing, The University of Texas at Austin
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
package gridlocate

import scala.collection.mutable

import util.collection._
import util.error.warning
import util.print.errprint
import langmodel.Gram

/**
 * A class used to implement user-frequency calculations. We are passed in
 * tuples of (term, user) and eventually can retrieve counts of unique
 * users for a given term. For efficiency, both term and user must
 * be given as integers; use memoization as necessary to map them down.
 */
class TermUserModel {
  // For each cell, we have a set listing the term-user pairs seen. We
  // encode them as a Long for speed and memory savings.
  val term_user_pairs = mutable.Set[Long]()

  def isEmpty = term_user_pairs.isEmpty
  def size = term_user_pairs.size
  def clear() { term_user_pairs.clear }

  /**
   * Record an occurrence of a given term token seen, along with the user
   * generating the token. Return true if we added the term/user combo,
   * false if already seen.
   */
  def add_term_user(term: Gram, user: Int) = {
    // This is a bit tricky. We need to combine two ints into a long, but to do
    // this properly we need to treat the int that goes into the lower 32 bits
    // as unsigned. Java (hence Scala) doesn't provide unsigned integers, but
    // an unsigned cast to long can be faked by anding the int with
    // 0xFFFFFFFFL, which effectively converts the int to a long and then
    // strips off the upper 32 bits, which get set if the int is negative.
    // These issues don't arise with the int that goes into the higher
    // portion of the long, nor do they arise converting back to ints.
    val entry = (term.toLong << 32L) + (user & 0xFFFFFFFFL)
    val present = term_user_pairs contains entry
    if (present)
      false
    else {
      term_user_pairs += entry
      true
    }
  }

  /**
   * Return a map from memoized words to counts of the unique users for a
   * given word in a given cell.
   */
  def get_term_counts = {
    val map = intmap[Gram]()
    val terms = term_user_pairs.map {
      pair => (pair >> 32).toInt
    }
    for (term <- terms)
      map(term) += 1
    map
  }
}

