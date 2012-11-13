///////////////////////////////////////////////////////////////////////////////
//  SphereCellDist.scala
//
//  Copyright (C) 2011 Ben Wing, The University of Texas at Austin
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

import math._

import opennlp.textgrounder.util.distances._

import opennlp.textgrounder.gridlocate.{WordCellDist,CellDistFactory}
import opennlp.textgrounder.worddist.WordDist.memoizer._

/////////////////////////////////////////////////////////////////////////////
//                             Cell distributions                          //
/////////////////////////////////////////////////////////////////////////////

/**
 * This is the Sphere-specific version of `WordCellDist`, which is used for
 * a distribution over cells that is associated with a single word.  This
 * is used in particular for the strategies and Geolocate applications that
 * need to invert the per-cell word distributions to obtain a per-word cell
 * distribution: Specifically, the GenerateKML app (which generates a KML
 * file showing the cell distribution of a given word across the Earth);
 * the `average-cell-probability` strategy (which uses the inversion strategy
 * to obtain a distribution of cells for each word in a document, and then
 * averages them); and the `most-common-toponym` baseline strategy (which
 * similarly uses the inversion strategy but obtains a single distribution of
 * cells for the most common toponym in the document, and uses this
 * distribution directly to generate the list of ranked cells).

 * Instances of this class are normally generated by the
 * `SphereCellDistFactory` class, which handles caching of common words
 * (which may get requested multiple times across a set of documents).
 * The above cases generally create a factory and then request individual
 * `SphereWordCellDist` objects using `get_cell_dist`, which may call
 * `create_word_cell_dist` to create the actual `SphereWordCellDist`.
 *
 * @param word Word for which the cell is computed
 * @param cellprobs Hash table listing probabilities associated with cells
 */

class SphereWordCellDist(
  cell_grid: SphereCellGrid,
  word: Word
) extends WordCellDist[SphereCoord](
  cell_grid, word) {
  // Convert cell to a KML file showing the distribution
  def generate_kml_file(filename: String, params: KMLParameters) {
    val xform = if (params.kml_transform == "log") (x: Double) => log(x)
    else if (params.kml_transform == "logsquared") (x: Double) => -log(x) * log(x)
    else (x: Double) => x

    val xf_minprob = xform(cellprobs.values min)
    val xf_maxprob = xform(cellprobs.values max)

    def yield_cell_kml() = {
      for {
        (cell, prob) <- cellprobs
        kml <- cell.asInstanceOf[KMLSphereCell].generate_kml(
          xform(prob), xf_minprob, xf_maxprob, params)
        expr <- kml
      } yield expr
    }

    val allcellkml = yield_cell_kml()

    val kml =
      <kml xmlns="http://www.opengis.net/kml/2.2" xmlns:gx="http://www.google.com/kml/ext/2.2" xmlns:kml="http://www.opengis.net/kml/2.2" xmlns:atom="http://www.w3.org/2005/Atom">
        <Document>
          <Style id="bar">
            <PolyStyle>
              <outline>0</outline>
            </PolyStyle>
            <IconStyle>
              <Icon/>
            </IconStyle>
          </Style>
          <Style id="downArrowIcon">
            <IconStyle>
              <Icon>
                <href>http://maps.google.com/mapfiles/kml/pal4/icon28.png</href>
              </Icon>
            </IconStyle>
          </Style>
          <Folder>
            <name>{ unmemoize_string(word) }</name>
            <open>1</open>
            <description>{ "Cell distribution for word '%s'" format unmemoize_string(word) }</description>
            <LookAt>
              <latitude>42</latitude>
              <longitude>-102</longitude>
              <altitude>0</altitude>
              <range>5000000</range>
              <tilt>53.454348562403</tilt>
              <heading>0</heading>
            </LookAt>
            { allcellkml }
          </Folder>
        </Document>
      </kml>

    xml.XML.save(filename, kml)
  }
}

class SphereCellDistFactory(
    lru_cache_size: Int
) extends CellDistFactory[SphereCoord](
    lru_cache_size) {
  type TCellDist = SphereWordCellDist
  def create_word_cell_dist(cell_grid: TGrid, word: Word) =
    new TCellDist(cell_grid, word)
}

