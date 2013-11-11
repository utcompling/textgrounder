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

import util.spherical.spheredist
import util.spherical.SphereCoord
import util.experiment._
import util.print.{errprint, warning}
import util.textdb.Row

import gridlocate.{GridDocFactory, DocStatus}

class CombinedModelGrid(
  docfact: SphereDocFactory, models: Seq[SphereGrid]
) extends SphereGrid(docfact) {

  override var total_num_cells: Int = models.map(_.total_num_cells).sum

  def find_best_cell_for_coord(coord: SphereCoord,
      create_non_recorded: Boolean) = {
    val candidates =
      models.flatMap(_.find_best_cell_for_coord(coord, create_non_recorded))
    if (candidates.length == 0)
      None
    else
      Some(candidates.minBy(
        cell => spheredist(cell.get_central_point, coord)))
  }

  def add_training_documents_to_grid(
      get_rawdocs: String => Iterator[DocStatus[Row]]) {
    // FIXME! This is broken because it ends up trying to compute the
    // smoothing statistics multiple times (each time it iterates over the
    // documents). We need to do this only once (probably after populating
    // the grid). In reality we need to separate out the code that handles
    // smoothing and other global mods. 
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


