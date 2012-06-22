///////////////////////////////////////////////////////////////////////////////
//  Permute.scala
//
//  Copyright (C) 2012 Stephen Roller, The University of Texas at Austin
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

package opennlp.textgrounder.preprocess

import util.Random
import com.nicta.scoobi.Scoobi._
import java.io._

/*
 * This program randomly permutes all the lines in a text file, using Hadoop
 * and Scoobi.
 */

object Permute extends ScoobiApp {
  val rnd = new Random

  def generate_key(line: String): (Double, String) = {
    (rnd.nextDouble, line)
  }

  def remove_key(kvs: (Double, Iterable[String])): Iterable[String] = {
    val (key, values) = kvs
    for (v <- values)
      yield v
  }

  def run() {
    // make sure we get all the input
    val (inputPath, outputPath) =
      if (args.length == 2) {
        (args(0), args(1))
      } else {
        sys.error("Expecting input and output path.")
      }

    // Firstly we load up all the (new-line-seperated) json lines
    val lines: DList[String] = TextInput.fromTextFile(inputPath)

    // randomly generate keys
    val with_keys = lines.map(generate_key)

    // sort by keys
    val keys_sorted = with_keys.groupByKey

    // remove keys
    val keys_removed = keys_sorted.flatMap(remove_key)

    // save to disk
    persist(TextOutput.toTextFile(keys_removed, outputPath))

  }
}

