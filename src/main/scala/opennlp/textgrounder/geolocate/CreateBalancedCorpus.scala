///////////////////////////////////////////////////////////////////////////////
//  CreateBalancedCorpus.scala
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

import scala.util.Random

import util.argparser._
import util.collection.bufmap
import util.experiment._
import util.metering._
import util.print.errprint
import util.spherical.SphereCoord
import util.textdb._

import gridlocate._

/**
 * Create a balanced corpus by ensuring that there are a maximum number of
 * users per cell. Reads in a corpus (consisting of three textdb databases,
 * one each for training, dev, test), creates a multi-regular grid, assigns
 * documents to the grid and then truncates the grid cells to contain at
 * most '--max-docs-per-cell' documents, then writes out a new corpus.
 */
class CreateBalancedCorpusParameters(
  parser: ArgParser
) extends GeolocateParameters(parser) {
  var output =
    ap.option[String]("o", "output",
      metavar = "FILE",
      must = be_specified,
      help = """File prefix of written-out cells.  Cells are written as a
textdb corpus, i.e. for each of training, dev and test two files will be
written, formed by adding (e.g. for the training set) `WORD-training.data.txt`
and `WORD-training.schema.txt` to the prefix, with the former storing the
data as tab-separated fields and the latter naming the fields.""")
  var max_docs_per_cell =
    ap.option[Int]("mdpc", "max-docs-per-cell",
      metavar = "COUNT",
      must = be_>(0),
      default = 100,
      help = """Maximum number of documents per cell in the written-out
corpus.""")
}

class CreateBalancedCorpusDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = CreateBalancedCorpusParameters
  type TRunRes = Unit

  def run() {
    val grid =
      create_empty_grid(create_document_factory_from_params, "balanced")
    assert(grid.isInstanceOf[MultiRegularGrid],
      s"Only works on regular grids: $grid")
    val multigrid = grid.asInstanceOf[MultiRegularGrid]
    val sets = Seq("training", "dev", "test")
    for (set <- sets) {
      // Record documents seen in each cell
      val docs_in_cell = bufmap[SphereCell, RawDoc]()
      (params.input ++ params.train).zipWithIndex.foreach {
        case (dir, index) => {
          val id = s"#${index + 1}"
          val task = show_progress(s"$id creating balanced corpus from",
            s"$set document")
          val rawdocs_statuses = GridDocFactory.read_raw_documents_from_textdb(
            get_file_handler, dir, s"-$set",
            importance = 1.0,
            with_messages = params.verbose)
          // FIXME: Should there be an easier way to process through the
          // DocStatus wrappers when we don't much care about the statuses?
          val rawdocs = new DocCounterTrackerFactory[RawDoc](this).
            process_statuses(rawdocs_statuses)
          rawdocs.foreachMetered(task) { rawdoc =>
            val coord = rawdoc.row.get[SphereCoord]("coord")
            // FIXME: We depend on MultiRegularCell-specific ways of creating
            // cells as necessary.
            val index = multigrid.coord_to_multi_cell_index(coord)
            val cell = multigrid.find_cell_for_cell_index(index, create = true,
              record_created_cell = true).get
            docs_in_cell(cell) += rawdoc
          }
        }
      }

      /***** Output statistics, including which cells get truncated ******/
      errprint("For set %s:", set)
      val num_docs = docs_in_cell.map {
        case (cell, buf) => (cell, buf.size)
      }
      for ((cell, count) <- num_docs.toSeq.sortWith(_._2 > _._2)) {
        if (count > params.max_docs_per_cell)
          errprint("Cell %s: Count = %s, truncated to %s", cell.format_location,
            count, params.max_docs_per_cell)
        else
          errprint("Cell %s: Count = %s", cell. format_location, count)
      }

      /***** Truncate cells, write them out. ******/
      val truncated = docs_in_cell.map {
        case (cell, buf) => {
          if (buf.size <= params.max_docs_per_cell)
            (cell, buf)
          else {
            // Choose randomly among the documents to be kept
            val trunc =
              (new Random()).shuffle(buf).take(params.max_docs_per_cell)
            (cell, trunc)
          }
        }
      }
      // Write out the docs in a random order rather than grouped by cell
      val rows = (new Random()).shuffle(truncated.flatMap(_._2).map(_.row))
      TextDB.write_textdb_rows(util.io.localfh, params.output + s"-$set",
        rows.iterator)
    }
  }
}

object CreateBalancedCorpus extends GeolocateApp("CreateBalancedCorpus") {
  type TDriver = CreateBalancedCorpusDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver = new TDriver
}

