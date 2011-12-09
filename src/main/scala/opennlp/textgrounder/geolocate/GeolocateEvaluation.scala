///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010, 2011 Ben Wing, The University of Texas at Austin
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

////////
//////// GeolocateEvaluation.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.printutil.{errprint}

/**
 * Abstract class for reading documents from a test file and evaluating
 * on them.
 *
 * @tparam TEvalDoc Type of document to evaluate.
 * @tparam TEvalRes Type of result of evaluating a document.
 *
 * Evaluates on all of the given files, outputting periodic results and
 * results after all files are done.  If the evaluator uses documents as
 * documents (so that it doesn't need any external test files), the value
 * of 'files' should be a sequence of one item, which is null. (If an
 * empty sequence is passed in, no evaluation will happen.)

 * Also returns an object containing the results.
 */
abstract class GeolocateTestFileEvaluator[TEvalDoc, TEvalRes](
  stratname: String,
  val driver: GeolocateDriver
) extends TestFileEvaluator[TEvalDoc, TEvalRes](stratname) {
  override val task = new MeteredTask("document", "evaluating",
    maxtime = driver.params.max_time_per_stage)
  var skip_initial = driver.params.skip_initial_test_docs
  var skip_n = 0

  override def would_skip_by_parameters() = {
    var do_skip = false
    if (skip_initial != 0) {
      skip_initial -= 1
      do_skip = true
    } else if (skip_n != 0) {
      skip_n -= 1
      do_skip = true
    } else
      skip_n = driver.params.every_nth_test_doc - 1
    do_skip
  }
        
  override def would_stop_processing(new_processed: Int) = {
    // If max # of docs reached, stop
    val stop = (driver.params.num_test_docs > 0 &&
                 new_processed >= driver.params.num_test_docs)
    if (stop) {
      errprint("")
      errprint("Stopping because limit of %s documents reached",
        driver.params.num_test_docs)
    }
    stop
  }
}

abstract class GeolocateDocumentEvaluator[
  TCoord,
  TDoc <: DistDocument[TCoord],
  TCell <: GeoCell[TCoord, TDoc],
  TGrid <: CellGrid[TCoord, TDoc, TCell],
  TEvalDoc,
  TEvalRes
](
  val strategy: GeolocateDocumentStrategy[TCoord, TDoc, TCell, TGrid],
  stratname: String,
  driver: GeolocateDocumentTypeDriver
) extends GeolocateTestFileEvaluator[TEvalDoc, TEvalRes](stratname, driver) {
  type TGroupedEvalStats <:
    GroupedDocumentEvalStats[TCoord,TDoc,TCell]
  def create_grouped_eval_stats(driver: GeolocateDocumentTypeDriver,
    cell_grid: TGrid, results_by_range: Boolean):
    TGroupedEvalStats
  val evalstats = create_grouped_eval_stats(driver,
    strategy.cell_grid, results_by_range = driver.params.results_by_range)

  def output_results(isfinal: Boolean = false) {
    evalstats.output_results(all_results = isfinal)
  }
}
