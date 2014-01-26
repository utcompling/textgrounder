///////////////////////////////////////////////////////////////////////////////
//  Ranker.scala
//
//  Copyright (C) 2012-2014 Ben Wing, The University of Texas at Austin
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
package learning

import util.collection.is_reverse_sorted

/**
 * A basic ranker. This is a machine-learning object that is given a query
 * item and a set of possible candidates and ranks the candidates by
 * determining a score for each one (on an arbitrary scale). The terminology
 * of queries and candidates from the "learning to rank" field within the
 * larger field of information retrieval. A paradigmatic example is a search
 * engine, where the query is the string typed into the search engine, the
 * candidates are a set of possibly relevant documents, and the result of
 * ranking should be an ordered list of the documents, from most to least
 * relevant.
 *
 * For the GridLocate application, a query is a document, a candidate is a
 * cell, and the ranker determines which cells are most likely to be the
 * correct ones.
 *
 * The objective function used for typical search-engine ranking is rather
 * different from what is used in GridLocate. In the former case we care
 * about all the candidates (potentially relevant documents) near the top of
 * the ranked list, where in the latter case we only really care about the
 * top-ranked candidate, i.e. cell (and moreover, we normally score an
 * erroneous top-ranked cell not simply by the fact that it is wrong but
 * how wrong it is, i.e. the distance between the cell's centroid location
 * and the document's actual location).
 */
trait Ranker[Query, Candidate] {
  /** Implementation of `evaluate`, to be provided by subclasses. */
  def imp_evaluate(item: Query, correct: Candidate,
      include_correct: Boolean):
    Iterable[(Candidate, Double)]

  /**
   * Evaluate a query item, returning a list of ranked candidates from best to
   * worst, with a score for each.  The score must not increase from any
   * candidate to the next one.
   *
   * @param correct Correct candidate, for oracles, etc.
   * @param include_correct If true, the correct candidate must be included
   *   in the returned list, ranked as if it were just any other candidate.
   *   FIXME: Why is this necessary? I think the reason the correct candidate
   *   might not be included is that it might be an empty cell. The place
   *   where 'include_correct' is set is in the reranker when it generates the
   *   initial ranking. (FIXME: But we skip training items where the cell is
   *   empty, in external_instances_to_query_candidate_pairs() ...)
   */
  final def evaluate(item: Query, correct: Candidate,
      include_correct: Boolean) = {
    val scored_cands = imp_evaluate(item, correct, include_correct)
    assert(is_reverse_sorted(scored_cands.map(_._2)),
      s"Candidates should be reverse-sorted: $scored_cands")
    scored_cands
  }
}
