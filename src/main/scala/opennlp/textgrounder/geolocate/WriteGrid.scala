///////////////////////////////////////////////////////////////////////////////
//  WriteGrid.scala
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
package geolocate

import util.argparser._
import util.experiment._
import util.textdb.TextDB

class WriteGridParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
  var output =
    ap.option[String]("o", "output",
      metavar = "FILE",
      must = be_specified,
      help = """File prefix of written-out cells.  Cells are written as a
textdb corpus, i.e. two files will be written, formed by adding
`WORD.data.txt` and `WORD.schema.txt` to the prefix, with the former
storing the data as tab-separated fields and the latter naming the fields.""")
}

class WriteGridDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = WriteGridParameters
  type TRunRes = Unit

  /**
   * Generate the salience grid.
   */

  def run() {
    val grid = initialize_grid
    val rows = for (cell <- grid.iter_nonempty_cells) yield cell.to_row
    note_result("textdb-type", "textgrounder-grid")
    write_textdb_values_with_results(util.io.localfh, params.output,
      rows.iterator)
  }
}

object WriteGrid extends GeolocateApp("WriteGrid") {
  type TDriver = WriteGridDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

