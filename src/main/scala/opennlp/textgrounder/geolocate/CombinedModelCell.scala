///////////////////////////////////////////////////////////////////////////////
//  CombinedModelGrid.scala
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

package opennlp.textgrounder
package geolocate

import util.distances.spheredist
import util.distances.SphereCoord
import util.experiment._
import util.print.{errprint, warning}

import gridlocate.GeoDocFactory
import gridlocate.{DocStatus, RawDocument}

class CombinedModelGrid(
  docfact: SphereDocFactory, models: Seq[SphereGrid]
) extends SphereGrid(docfact) {

  override var total_num_cells: Int = models.map(_.total_num_cells).sum

  def find_best_cell_for_document(doc: SphereDoc,
                                  create_non_recorded: Boolean) = {
      val candidates =
        models.flatMap(_.find_best_cell_for_document(doc, create_non_recorded))
      if (candidates.length == 0)
        None
      else
        Some(candidates.minBy(
          cell => spheredist(cell.get_center_coord, doc.coord)))
  }

  def add_training_documents_to_grid(
      get_rawdocs: String => Iterator[DocStatus[RawDocument]]) {
    for (model <- models)
      model.add_training_documents_to_grid(get_rawdocs)
  }

  protected def initialize_cells() {
    // Should never be called because we override finish().
    assert(false)
  }

  override def finish() {
    for (model <- models) {
      model.finish()
    }
    num_non_empty_cells = models.map(_.num_non_empty_cells).sum
  }

  def iter_nonempty_cells: Iterable[SphereCell] =
    models.flatMap(_.iter_nonempty_cells)
}


