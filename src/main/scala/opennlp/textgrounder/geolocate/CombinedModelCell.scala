///////////////////////////////////////////////////////////////////////////////
//  CombinedModelCellGrid.scala
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

package opennlp.textgrounder.geolocate

import opennlp.textgrounder.util.distances.spheredist
import opennlp.textgrounder.util.distances.SphereCoord
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.printutil.{errprint, warning}

class CombinedModelCellGrid(table: SphereDocumentTable,
                            models: Seq[SphereCellGrid])
    extends SphereCellGrid(table) {

  override var total_num_cells: Int = models.map(_.total_num_cells).sum
  override val num_training_passes: Int = models.map(_.num_training_passes).max

  var current_training_pass: Int = 0

  override def begin_training_pass(pass: Int) = {
    current_training_pass = pass
    for (model <- models) {
      if (pass <= model.num_training_passes) {
        model.begin_training_pass(pass)
      }
    }
  }

  def find_best_cell_for_coord(coord: SphereCoord,
                               create_non_recorded: Boolean) = {
      val candidates = models.map(_.find_best_cell_for_coord(coord, create_non_recorded))
                             .filter(_ != null)
      candidates.minBy((cell: SphereCell) =>
                         spheredist(cell.get_center_coord, coord))
  }

  def add_document_to_cell(document: SphereDocument) {
    for (model <- models) {
      if (current_training_pass <= model.num_training_passes) {
        model.add_document_to_cell(document)
      }
    }
  }

  def initialize_cells() {
  }

  override def finish() {
    for (model <- models) {
      model.finish()
    }
    num_non_empty_cells = models.map(_.num_non_empty_cells).sum
  }

  def iter_nonempty_cells(nonempty_word_dist: Boolean = false): Iterable[SphereCell] = {
    models.map(_.iter_nonempty_cells(nonempty_word_dist))
          .reduce(_ ++ _)
  }
}


