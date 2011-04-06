///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Travis Brown, The University of Texas at Austin
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
package opennlp.textgrounder.resolver

import scala.collection.JavaConversions._
import opennlp.textgrounder.text._


class AltBasicMinDistResolver extends Resolver {
  def disambiguate(corpus: StoredCorpus): StoredCorpus = {

    /* Iterate over documents. */
    corpus.foreach { document =>

      /* Collect a list of toponyms with candidates for each document. */
      val toponyms = document.flatMap(_.getToponyms).filter(_.getAmbiguity > 0).toList

      /* For each toponym, pick the best candidate. */
      toponyms.foreach { toponym =>

        /* Compute all valid totals with indices. */
        toponym.zipWithIndex.flatMap { case (candidate, idx) =>
          toponyms.filterNot(_ == toponym) match {
            case Nil => None
            case ts  => Some(ts.map(_.map(_.distance(candidate)).min).sum, idx)
          }
        } match {
          case Nil => ()
          case xs  => toponym.setSelectedIdx(xs.min._2)
        }
      }
    }

    return corpus
  }
}

