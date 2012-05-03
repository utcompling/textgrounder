///////////////////////////////////////////////////////////////////////////////
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

package opennlp.textgrounder.geolocate.toponym

import collection.mutable
import util.control.Breaks._
import math._

import opennlp.textgrounder.util.argparser._
import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.distances
import opennlp.textgrounder.util.distances.SphereCoord
import opennlp.textgrounder.util.distances.spheredist
import opennlp.textgrounder.util.experiment._
import opennlp.textgrounder.util.ioutil.{FileHandler, Schema}
import opennlp.textgrounder.util.osutil._
import opennlp.textgrounder.util.printutil.{errout, errprint, warning}

import opennlp.textgrounder.gridlocate.{CombinedWordDist,EvalStats,CorpusEvaluator,DocumentIteratingEvaluator}
import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._
import opennlp.textgrounder.geolocate._

import opennlp.textgrounder.worddist.{WordDist,WordDistFactory}
import opennlp.textgrounder.worddist.WordDist.memoizer._

/* FIXME: Eliminate this. */
import GeolocateToponymApp.Params

// A class holding the boundary of a geographic object.  Currently this is
// just a bounding box, but eventually may be expanded to including a
// convex hull or more complex model.

class Boundary(botleft: SphereCoord, topright: SphereCoord) {
  override def toString = {
    "%s-%s" format (botleft, topright)
  }

  // def __repr__()  = {
  //   "Boundary(%s)" format toString
  // }

  def struct = <Boundary boundary={ "%s-%s" format (botleft, topright) }/>

  def contains(coord: SphereCoord) = {
    if (!(coord.lat >= botleft.lat && coord.lat <= topright.lat))
      false
    else if (botleft.long <= topright.long)
      (coord.long >= botleft.long && coord.long <= topright.long)
    else {
      // Handle case where boundary overlaps the date line.
      (coord.long >= botleft.long &&
        coord.long <= topright.long + 360.) ||
        (coord.long >= botleft.long - 360. &&
          coord.long <= topright.long)
    }
  }

  def square_area() = distances.square_area(botleft, topright)

  /**
   * Iterate over the cells that overlap the boundary.
   *
   * @param cell_grid FIXME: Currently we need a cell grid of a certain type
   *    passed in so we can iterate over the cells.  Fix this so that
   *    the algorithm is done differently, or something.
   */
  def iter_nonempty_tiling_cells(cell_grid: TopoCellGrid) = {
    val botleft_index = cell_grid.coord_to_tiling_cell_index(botleft)
    val (latind1, longind1) = (botleft_index.latind, botleft_index.longind)
    val topright_index = cell_grid.coord_to_tiling_cell_index(topright)
    val (latind2, longind2) = (topright_index.latind, topright_index.longind)
    for {
      i <- latind1 to latind2 view
      val it = if (longind1 <= longind2) longind1 to longind2 view
      else (longind1 to cell_grid.maximum_longind view) ++
        (cell_grid.minimum_longind to longind2 view)
      j <- it
      val index = RegularCellIndex(i, j)
      if (cell_grid.tiling_cell_to_documents contains index)
    } yield index
  }
}

///////////// Locations ////////////

// A general location (either locality or division).  The following
// fields are defined:
//
//   name: Name of location.
//   altnames: List of alternative names of location.
//   typ: Type of location (locality, agglomeration, country, state,
//                           territory, province, etc.)
//   docmatch: Document corresponding to this location.
//   div: Next higher-level division this location is within, or None.

abstract class Location(
  val name: String,
  val altnames: Seq[String],
  val typ: String
) {
  var docmatch: TopoDocument = null
  var div: Division = null
  def toString(no_document: Boolean = false): String
  def shortstr: String
  def struct(no_document: Boolean = false): xml.Elem
  def distance_to_coord(coord: SphereCoord): Double
  def matches_coord(coord: SphereCoord): Boolean
}

// A location corresponding to an entry in a gazetteer, with a single
// coordinate.
//
// The following fields are defined, in addition to those for Location:
//
//   coord: Coordinates of the location, as a SphereCoord object.

class Locality(
  name: String,
  val coord: SphereCoord,
  altnames: Seq[String],
  typ: String
) extends Location(name, altnames, typ) {

  def toString(no_document: Boolean = false) = {
    var docmatch = ""
    if (!no_document)
      docmatch = ", match=%s" format docmatch
    "Locality %s (%s) at %s%s" format (
      name, if (div != null) div.path.mkString("/") else "unknown",
      coord, docmatch)
  }

  // def __repr__() = {
  //   toString.encode("utf-8")
  // }

  def shortstr = {
    "Locality %s (%s)" format (
      name, if (div != null) div.path.mkString("/") else "unknown")
  }

  def struct(no_document: Boolean = false) =
    <Locality>
      <name>{ name }</name>
      <inDivision>{ if (div != null) div.path.mkString("/") else "" }</inDivision>
      <atCoordinate>{ coord }</atCoordinate>
      {
        if (!no_document)
          <matching>{ if (docmatch != null) docmatch.struct else "none" }</matching>
      }
    </Locality>

  def distance_to_coord(coord: SphereCoord) = spheredist(coord, coord)

  def matches_coord(coord: SphereCoord) = {
    distance_to_coord(coord) <= Params.max_dist_for_close_match
  }
}

// A division higher than a single locality.  According to the World
// gazetteer, there are three levels of divisions.  For the U.S., this
// corresponds to country, state, county.
//
class Division(
  // Tuple of same size as the level #, listing the path of divisions
  // from highest to lowest, leading to this division.  The last
  // element is the same as the "name" of the division.
  val path: Seq[String]
) extends Location(path(path.length - 1), Seq[String](), "unknown") {

  // 1, 2, or 3 for first, second, or third-level division
  val level = path.length
  // List of locations inside of the division.
  var locs = mutable.Buffer[Locality]()
  // List of locations inside of the division other than those
  // rejected as outliers (too far from all other locations).
  var goodlocs = mutable.Buffer[Locality]()
  // Boundary object specifying the boundary of the area of the
  // division.  Currently in the form of a rectangular bounding box.
  // Eventually may contain a convex hull or even more complex
  // cell (e.g. set of convex cells).
  var boundary: Boundary = null
  // For cell-based Naive Bayes disambiguation, a distribution
  // over the division's document and all locations within the cell.
  var combined_dist: CombinedWordDist = null

  def toString(no_document: Boolean = false) = {
    val docmatchstr =
      if (no_document) "" else ", match=%s" format docmatch
    "Division %s (%s)%s, boundary=%s" format (
      name, path.mkString("/"), docmatchstr, boundary)
  }

  // def __repr__() = toString.encode("utf-8")

  def shortstr = {
    ("Division %s" format name) + (
      if (level > 1) " (%s)" format (path.mkString("/")) else "")
  }

  def struct(no_document: Boolean = false): xml.Elem =
    <Division>
      <name>{ name }</name>
      <path>{ path.mkString("/") }</path>
      {
        if (!no_document)
          <matching>{ if (docmatch != null) docmatch.struct else "none" }</matching>
      }
      <boundary>{ boundary.struct }</boundary>
    </Division>

  def distance_to_coord(coord: SphereCoord) = java.lang.Double.NaN

  def matches_coord(coord: SphereCoord) = this contains coord

  // Compute the boundary of the geographic cell of this division, based
  // on the points in the cell.
  def compute_boundary() {
    // Yield up all points that are not "outliers", where outliers are defined
    // as points that are more than Params.max_dist_for_outliers away from all
    // other points.
    def iter_non_outliers() = {
      // If not enough points, just return them; otherwise too much possibility
      // that all of them, or some good ones, will be considered outliers.
      if (locs.length <= 5) {
        for (p <- locs) yield p
      } else {
        // FIXME: Actually look for outliers.
        for (p <- locs) yield p
        //for {
        //  p <- locs
        //  // Find minimum distance to all other points and check it.
        //  mindist = (for (x <- locs if !(x eq p)) yield spheredist(p, x)) min
        //  if (mindist <= Params.max_dist_for_outliers)
        //} yield p
      }
    }

    if (debug("lots")) {
      errprint("Computing boundary for %s, path %s, num points %s",
        name, path, locs.length)
    }

    goodlocs = iter_non_outliers()
    // If we've somehow discarded all points, just use the original list
    if (goodlocs.length == 0) {
      if (debug("some")) {
        warning("All points considered outliers?  Division %s, path %s",
          name, path)
      }
      goodlocs = locs
    }
    // FIXME! This will fail for a division that crosses the International
    // Date Line.
    val topleft = SphereCoord((for (x <- goodlocs) yield x.coord.lat) min,
      (for (x <- goodlocs) yield x.coord.long) min)
    val botright = SphereCoord((for (x <- goodlocs) yield x.coord.lat) max,
      (for (x <- goodlocs) yield x.coord.long) max)
    boundary = new Boundary(topleft, botright)
  }

  def generate_word_dist(word_dist_factory: WordDistFactory) {
    combined_dist = new CombinedWordDist(word_dist_factory)
    for (loc <- Seq(this) ++ goodlocs if loc.docmatch != null)
      yield combined_dist.add_document(loc.docmatch)
    combined_dist.word_dist.finish_before_global()
    combined_dist.word_dist.finish_after_global()
  }

  def contains(coord: SphereCoord) = boundary contains coord
}

class DivisionFactory(gazetteer: Gazetteer) {
  // For each division, map from division's path to Division object.
  val path_to_division = mutable.Map[Seq[String], Division]()

  // For each tiling cell, list of divisions that have territory in it
  val tiling_cell_to_divisions = bufmap[RegularCellIndex, Division]()

  // Find the division for a point in the division with a given path,
  // add the point to the division.  Create the division if necessary.
  // Return the corresponding Division.
  def find_division_note_point(loc: Locality, path: Seq[String]): Division = {
    val higherdiv = if (path.length > 1)
      // Also note location in next-higher division.
      find_division_note_point(loc, path.dropRight(1))
    else null
    // Skip divisions where last element in path is empty; this is a
    // reference to a higher-level division with no corresponding lower-level
    // division.
    if (path.last.length == 0) higherdiv
    else {
      val division = {
        if (path_to_division contains path)
          path_to_division(path)
        else {
          // If we haven't seen this path, create a new Division object.
          // Record the mapping from path to division, and also from the
          // division's "name" (name of lowest-level division in path) to
          // the division.
          val newdiv = new Division(path)
          newdiv.div = higherdiv
          path_to_division(path) = newdiv
          gazetteer.record_division(path.last.toLowerCase, newdiv)
          newdiv
        }
      }
      division.locs += loc
      division
    }
  }

  /**
   * Finish all computations related to Divisions, after we've processed
   * all points (and hence all points have been added to the appropriate
   * Divisions).
   */
  def finish_all() {
    val divs_by_area = mutable.Buffer[(Division, Double)]()
    for (division <- path_to_division.values) {
      if (debug("lots")) {
        errprint("Processing division named %s, path %s",
          division.name, division.path)
      }
      division.compute_boundary()
      val docmatch = gazetteer.cell_grid.table.asInstanceOf[TopoDocumentTable].
        topo_subtable.find_match_for_division(division)
      if (docmatch != null) {
        if (debug("lots")) {
          errprint("Matched document %s for division %s, path %s",
            docmatch, division.name, division.path)
        }
        division.docmatch = docmatch
        docmatch.location = division
      } else {
        if (debug("lots")) {
          errprint("Couldn't find match for division %s, path %s",
            division.name, division.path)
        }
      }
      for (index <-
          division.boundary.iter_nonempty_tiling_cells(gazetteer.cell_grid))
        tiling_cell_to_divisions(index) += division
      if (debug("cell"))
        divs_by_area += ((division, division.boundary.square_area()))
    }
    if (debug("cell")) {
      // sort by second element of tuple, in reverse order
      for ((div, area) <- divs_by_area sortWith (_._2 > _._2))
        errprint("%.2f square km: %s", area, div)
    }
  }
}

class TopoCellGrid(
  degrees_per_cell: Double,
  width_of_multi_cell: Int,
  table: TopoDocumentTable
) extends MultiRegularCellGrid(degrees_per_cell, width_of_multi_cell, table) {

  /**
   * Mapping from tiling cell to documents in the cell.
   */
  var tiling_cell_to_documents = bufmap[RegularCellIndex, SphereDocument]()

  /**
   * Override so that we can keep a mapping of cell to documents in the cell.
   * FIXME: Do we really need this?
   */
  override def add_document_to_cell(doc: SphereDocument) {
    val index = coord_to_tiling_cell_index(doc.coord)
    tiling_cell_to_documents(index) += doc
    super.add_document_to_cell(doc)
  }
}

class TopoDocument(
  schema: Schema,
  subtable: TopoDocumentSubtable
) extends WikipediaDocument(schema, subtable) {
  // Cell-based distribution corresponding to this document.
  var combined_dist: CombinedWordDist = null
  // Corresponding location for this document.
  var location: Location = null

  override def toString = {
    var ret = super.toString
    if (location != null) {
      ret += (", matching location %s" format
        location.toString(no_document = true))
    }
    val divs = find_covering_divisions()
    val top_divs =
      for (div <- divs if div.level == 1)
        yield div.toString(no_document = true)
    val topdivstr =
    if (top_divs.length > 0)
        ", in top-level divisions %s" format (top_divs.mkString(", "))
      else
        ", not in any top-level divisions"
    ret + topdivstr
  }

  override def shortstr = {
    var str = super.shortstr
    if (location != null)
      str += ", matching %s" format location.shortstr
    val divs = find_covering_divisions()
    val top_divs = (for (div <- divs if div.level == 1) yield div.name)
    if (top_divs.length > 0)
      str += ", in top-level divisions %s" format (top_divs.mkString(", "))
    str
  }

  override def struct = {
    val xml = super.struct
    <TopoDocument>
      { xml.child }
      {
        if (location != null)
          <matching>{ location.struct(no_document = true) }</matching>
      }
      {
        val divs = find_covering_divisions()
        val top_divs = (for (div <- divs if div.level == 1)
          yield div.struct(no_document = true))
        if (top_divs != null)
          <topLevelDivisions>{ top_divs }</topLevelDivisions>
        else
          <topLevelDivisions>none</topLevelDivisions>
      }
    </TopoDocument>
  }

  def matches_coord(coord: SphereCoord) = {
    if (distance_to_coord(coord) <= Params.max_dist_for_close_match) true
    else if (location != null && location.isInstanceOf[Division] &&
      location.matches_coord(coord)) true
    else false
  }

  // Determine the cell word-distribution object for a given document:
  // Create and populate one if necessary.
  def find_combined_word_dist(cell_grid: SphereCellGrid) = {
    val loc = location
    if (loc != null && loc.isInstanceOf[Division]) {
      val div = loc.asInstanceOf[Division]
      if (div.combined_dist == null)
        div.generate_word_dist(cell_grid.table.word_dist_factory)
      div.combined_dist
    } else {
      if (combined_dist == null) {
        val cell = cell_grid.find_best_cell_for_coord(coord, false)
        if (cell != null)
          combined_dist = cell.combined_dist
        else {
          warning("Couldn't find existing cell distribution for document %s",
            this)
          combined_dist = new CombinedWordDist(table.word_dist_factory)
          combined_dist.word_dist.finish_before_global()
          combined_dist.word_dist.finish_after_global()
        }
      }
      combined_dist
    }
  }

  // Find the divisions that cover the given document.
  def find_covering_divisions() = {
    val inds = subtable.gazetteer.cell_grid.coord_to_tiling_cell_index(coord)
    val divs = subtable.gazetteer.divfactory.tiling_cell_to_divisions(inds)
    (for (div <- divs if div contains coord) yield div)
  }
}

// Static class maintaining additional tables listing mapping between
// names, ID's and documents.  See comments at WikipediaDocumentTable.
class TopoDocumentSubtable(
  val topo_table: TopoDocumentTable
) extends WikipediaDocumentSubtable(topo_table) {
  override def create_document(schema: Schema) =
    new TopoDocument(schema, this)

  var gazetteer: Gazetteer = null

  /**
   * Set the gazetteer.  Must do it this way because creation of the
   * gazetteer wants the TopoDocumentTable already created.
   */
  def set_gazetteer(gaz: Gazetteer) {
    gazetteer = gaz
  }

  // Construct the list of possible candidate documents for a given toponym
  override def construct_candidates(toponym: String) = {
    val lotop = toponym.toLowerCase
    val locs = (
      gazetteer.lower_toponym_to_location(lotop) ++
      gazetteer.lower_toponym_to_division(lotop))
    val documents = super.construct_candidates(toponym)
    documents ++ (
      for {loc <- locs
           if (loc.docmatch != null && !(documents contains loc.docmatch))}
        yield loc.docmatch
    )
  }

  override def word_is_toponym(word: String) = {
    val lw = word.toLowerCase
    (super.word_is_toponym(word) ||
      (gazetteer.lower_toponym_to_location contains lw) ||
      (gazetteer.lower_toponym_to_division contains lw))
  }

  // Find document matching name NAME for location LOC.  NAME will generally
  // be one of the names of LOC (either its canonical name or one of the
  // alternate name).  CHECK_MATCH is a function that is passed one arument,
  // the document, and should return true if the location matches the document.
  // PREFER_MATCH is used when two or more documents match.  It is passed
  // two arguments, the two documents.  It should return TRUE if the first is
  // to be preferred to the second.  Return the document matched, or None.

  def find_one_document_match(loc: Location, name: String,
    check_match: (TopoDocument) => Boolean,
    prefer_match: (TopoDocument, TopoDocument) => Boolean): TopoDocument = {

    val loname = memoize_string(name.toLowerCase)

    // Look for any documents with same name (case-insensitive) as the
    // location, check for matches
    for (wiki_doc <- lower_name_to_documents(loname);
         doc = wiki_doc.asInstanceOf[TopoDocument])
      if (check_match(doc)) return doc

    // Check whether there is a match for a document whose name is
    // a combination of the location's name and one of the divisions that
    // the location is in (e.g. "Augusta, Georgia" for a location named
    // "Augusta" in a second-level division "Georgia").
    if (loc.div != null) {
      for {
        div <- loc.div.path
        lodiv = memoize_string(div.toLowerCase)
        wiki_doc <- lower_name_div_to_documents((loname, lodiv))
        doc = wiki_doc.asInstanceOf[TopoDocument]
      } if (check_match(doc)) return doc
    }

    // See if there is a match with any of the documents whose short name
    // is the same as the location's name
    val docs = short_lower_name_to_documents(loname)
    if (docs != null) {
      val gooddocs =
        (for (wiki_doc <- docs;
              doc = wiki_doc.asInstanceOf[TopoDocument];
              if check_match(doc))
            yield doc)
      if (gooddocs.length == 1)
        return gooddocs(0) // One match
      else if (gooddocs.length > 1) {
        // Multiple matches: Sort by preference, return most preferred one
        if (debug("lots")) {
          errprint("Warning: Saw %s toponym matches: %s",
            gooddocs.length, gooddocs)
        }
        val sorteddocs = gooddocs sortWith (prefer_match(_, _))
        return sorteddocs(0)
      }
    }

    // No match.
    return null
  }

  // Find document matching location LOC.  CHECK_MATCH and PREFER_MATCH are
  // as above.  Return the document matched, or None.

  def find_document_match(loc: Location,
    check_match: (TopoDocument) => Boolean,
    prefer_match: (TopoDocument, TopoDocument) => Boolean): TopoDocument = {
    // Try to find a match for the canonical name of the location
    val docmatch = find_one_document_match(loc, loc.name, check_match,
      prefer_match)
    if (docmatch != null) return docmatch

    // No match; try each of the alternate names in turn.
    for (altname <- loc.altnames) {
      val docmatch2 = find_one_document_match(loc, altname, check_match,
        prefer_match)
      if (docmatch2 != null) return docmatch2
    }

    // No match.
    return null
  }

  // Find document matching locality LOC; the two coordinates must be at most
  // MAXDIST away from each other.

  def find_match_for_locality(loc: Locality, maxdist: Double) = {

    def check_match(doc: TopoDocument) = {
      val dist = spheredist(loc.coord, doc.coord)
      if (dist <= maxdist) true
      else {
        if (debug("lots")) {
          errprint("Found document %s but dist %s > %s",
            doc, dist, maxdist)
        }
        false
      }
    }

    def prefer_match(doc1: TopoDocument, doc2: TopoDocument) = {
      spheredist(loc.coord, doc1.coord) < spheredist(loc.coord, doc2.coord)
    }

    find_document_match(loc, check_match, prefer_match)
  }

  // Find document matching division DIV; the document coordinate must be
  // inside of the division's boundaries.

  def find_match_for_division(div: Division) = {

    def check_match(doc: TopoDocument) = {
      if (doc.has_coord && (div contains doc.coord)) true
      else {
        if (debug("lots")) {
          if (!doc.has_coord) {
            errprint("Found document %s but no coordinate, so not in location named %s, path %s",
              doc, div.name, div.path)
          } else {
            errprint("Found document %s but not in location named %s, path %s",
              doc, div.name, div.path)
          }
        }
        false
      }
    }

    def prefer_match(doc1: TopoDocument, doc2: TopoDocument) = {
      val l1 = doc1.incoming_links
      val l2 = doc2.incoming_links
      // Prefer according to incoming link counts, if that info is available
      if (l1 != None && l2 != None) l1.get > l2.get
      else {
        // FIXME: Do something smart here -- maybe check that location is
        // farther in the middle of the bounding box (does this even make
        // sense???)
        true
      }
    }

    find_document_match(div, check_match, prefer_match)
  }
}

/**
 * A version of SphereDocumentTable that substitutes a TopoDocumentSubtable
 * for the Wikipedia subtable.
 */
class TopoDocumentTable(
  val topo_driver: GeolocateToponymDriver,
  word_dist_factory: WordDistFactory
) extends SphereDocumentTable(
  topo_driver, word_dist_factory
) {
  val topo_subtable = new TopoDocumentSubtable(this)
  override val wikipedia_subtable = topo_subtable
}

class EvalStatsWithCandidateList(
  driver_stats: ExperimentDriverStats,
  prefix: String,
  incorrect_reasons: Map[String, String],
  max_individual_candidates: Int = 5
) extends EvalStats(driver_stats, prefix, incorrect_reasons) {

  def record_result(correct: Boolean, reason: String, num_candidates: Int) {
    super.record_result(correct, reason)
    increment_counter("instances.total.by_candidate." + num_candidates)
    if (correct)
      increment_counter("instances.correct.by_candidate." + num_candidates)
    else
      increment_counter("instances.incorrect.by_candidate." + num_candidates)
  }

  // SCALABUG: The need to write collection.Map here rather than simply
  // Map seems clearly wrong.  It seems the height of obscurity that
  // "collection.Map" is the common supertype of plain "Map"; the use of
  // overloaded "Map" seems to be the root of the problem.
  def output_table_by_num_candidates(group: String, total: Long) {
    for (i <- 0 to max_individual_candidates)
      output_fraction("  With %d  candidates" format i,
        get_counter(group + "." + i), total)
    val items = (
      for {counter <- list_counters(group, false, false)
           key = counter.toInt
           if key > max_individual_candidates
          }
      yield get_counter(group + "." + counter)
      ).sum
    output_fraction(
      "  With %d+ candidates" format (1 + max_individual_candidates),
      items, total)
  }

  override def output_correct_results() {
    super.output_correct_results()
    output_table_by_num_candidates("instances.correct", correct_instances)
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
    output_table_by_num_candidates("instances.incorrect", incorrect_instances)
  }
}

object GeolocateToponymResults {
  val incorrect_geolocate_toponym_reasons = Map(
    "incorrect_with_no_candidates" ->
      "Incorrect, with no candidates",
    "incorrect_with_no_correct_candidates" ->
      "Incorrect, with candidates but no correct candidates",
    "incorrect_with_multiple_correct_candidates" ->
      "Incorrect, with multiple correct candidates",
    "incorrect_one_correct_candidate_missing_link_info" ->
      "Incorrect, with one correct candidate, but link info missing",
    "incorrect_one_correct_candidate" ->
      "Incorrect, with one correct candidate")
}

//////// Results for geolocating toponyms
class GeolocateToponymResults(driver_stats: ExperimentDriverStats) {
  import GeolocateToponymResults._

  // Overall statistics
  val all_toponym = new EvalStatsWithCandidateList(
    driver_stats, "", incorrect_geolocate_toponym_reasons)
  // Statistics when toponym not same as true name of location
  val diff_surface = new EvalStatsWithCandidateList(
    driver_stats, "diff_surface", incorrect_geolocate_toponym_reasons)
  // Statistics when toponym not same as true name or short form of location
  val diff_short = new EvalStatsWithCandidateList(
    driver_stats, "diff_short", incorrect_geolocate_toponym_reasons)

  def record_geolocate_toponym_result(correct: Boolean, toponym: String,
      trueloc: String, reason: String, num_candidates: Int) {
    all_toponym.record_result(correct, reason, num_candidates)
    if (toponym != trueloc) {
      diff_surface.record_result(correct, reason, num_candidates)
      val (short, div) = WikipediaDocument.compute_short_form(trueloc)
      if (toponym != short)
        diff_short.record_result(correct, reason, num_candidates)
    }
  }

  def output_geolocate_toponym_results() {
    errprint("Results for all toponyms:")
    all_toponym.output_results()
    errprint("")
    errprint("Results for toponyms when different from true location name:")
    diff_surface.output_results()
    errprint("")
    errprint("Results for toponyms when different from either true location name")
    errprint("  or its short form:")
    diff_short.output_results()
    output_resource_usage()
  }
}

// Class of word in a file containing toponyms.  Fields:
//
//   word: The identity of the word.
//   is_stop: true if it is a stopword.
//   is_toponym: true if it is a toponym.
//   coord: For a toponym with specified ground-truth coordinate, the
//          coordinate.  Else, null.
//   location: true location if given, else null.
//   context: Vector including the word and 10 words on other side.
//   document: The document (document, etc.) of the word.  Useful when a single
//             file contains multiple such documents.
//
class GeogWord(val word: String) {
  var is_stop = false
  var is_toponym = false
  var coord: SphereCoord = null
  var location: String = null
  var context: Array[(Int, String)] = null
  var document: String = null
}

abstract class GeolocateToponymStrategy {
  def need_context(): Boolean
  def compute_score(geogword: GeogWord, doc: TopoDocument): Double
}

// Find each toponym explicitly mentioned as such and disambiguate it
// (find the correct geographic location) using the "link baseline", i.e.
// use the location with the highest number of incoming links.
class BaselineGeolocateToponymStrategy(
  cell_grid: SphereCellGrid,
  val baseline_strategy: String) extends GeolocateToponymStrategy {
  def need_context() = false

  def compute_score(geogword: GeogWord, doc: TopoDocument) = {
    val topo_table = cell_grid.table.asInstanceOf[TopoDocumentTable]
    val params = topo_table.topo_driver.params
    if (baseline_strategy == "internal-link") {
      if (params.context_type == "cell")
        doc.find_combined_word_dist(cell_grid).incoming_links
      else
        doc.adjusted_incoming_links
    } else if (baseline_strategy == "num-documents") {
      if (params.context_type == "cell")
        doc.find_combined_word_dist(cell_grid).num_docs_for_links
      else {
        val location = doc.location
        location match {
          case x:Division => x.locs.length
          case _ => 1
        }
      }
    } else random
  }
}

// Find each toponym explicitly mentioned as such and disambiguate it
// (find the correct geographic location) using Naive Bayes, possibly
// in conjunction with the baseline.
class NaiveBayesToponymStrategy(
  cell_grid: SphereCellGrid,
  val use_baseline: Boolean
) extends GeolocateToponymStrategy {
  def need_context() = true

  def compute_score(geogword: GeogWord, doc: TopoDocument) = {
    // FIXME FIXME!!! We are assuming that the baseline is "internal-link",
    // regardless of its actual settings.
    val thislinks = WikipediaDocument.log_adjust_incoming_links(
      doc.adjusted_incoming_links)
    val topo_table = cell_grid.table.asInstanceOf[TopoDocumentTable]
    val params = topo_table.topo_driver.params

    var distobj =
      if (params.context_type == "document") doc.dist
      else doc.find_combined_word_dist(cell_grid).word_dist
    var totalprob = 0.0
    var total_word_weight = 0.0
    val (word_weight, baseline_weight) =
      if (!use_baseline) (1.0, 0.0)
      else if (params.naive_bayes_weighting == "equal") (1.0, 1.0)
      else (1 - params.naive_bayes_baseline_weight,
            params.naive_bayes_baseline_weight)
    for ((dist, word) <- geogword.context) {
      val lword =
        if (params.preserve_case_words) word else word.toLowerCase
      val wordprob =
        distobj.lookup_word(WordDist.memoizer.memoize_string(lword))

      // Compute weight for each word, based on distance from toponym
      val thisweight =
        if (params.naive_bayes_weighting == "equal" ||
          params.naive_bayes_weighting == "equal-words") 1.0
        else 1.0 / (1 + dist)

      total_word_weight += thisweight
      totalprob += thisweight * log(wordprob)
    }
    if (debug("some"))
      errprint("Computed total word log-likelihood as %s", totalprob)
    // Normalize probability according to the total word weight
    if (total_word_weight > 0)
      totalprob /= total_word_weight
    // Combine word and prior (baseline) probability acccording to their
    // relative weights
    totalprob *= word_weight
    totalprob += baseline_weight * log(thislinks)
    if (debug("some"))
      errprint("Computed total log-likelihood as %s", totalprob)
    totalprob
  }
}

class ToponymEvaluationResult  { }
case class GeogWordDocument(words: Iterable[GeogWord])

abstract class GeolocateToponymEvaluator(
  strategy: GeolocateToponymStrategy,
  stratname: String,
  driver: GeolocateToponymDriver
) extends CorpusEvaluator[
  GeogWordDocument, ToponymEvaluationResult
](stratname, driver) with DocumentIteratingEvaluator[
  GeogWordDocument, ToponymEvaluationResult
] {
  val toponym_results = new GeolocateToponymResults(driver)

  // Given an evaluation file, read in the words specified, including the
  // toponyms.  Mark each word with the "document" (e.g. document) that it's
  // within.
  def iter_geogwords(filehand: FileHandler, filename: String): GeogWordDocument

  // Retrieve the words yielded by iter_geogwords() and separate by "document"
  // (e.g. document); yield each "document" as a list of such GeogWord objects.
  // If compute_context, also generate the set of "context" words used for
  // disambiguation (some window, e.g. size 20, of words around each
  // toponym).
  def iter_documents(filehand: FileHandler, filename: String) = {
    def return_word(word: GeogWord) = {
      if (word.is_toponym) {
        if (debug("lots")) {
          errprint("Saw loc %s with true coordinates %s, true location %s",
            word.word, word.coord, word.location)
        }
      } else {
        if (debug("tons"))
          errprint("Non-toponym %s", word.word)
      }
      word
    }

    for ((k, g) <- iter_geogwords(filehand, filename).words.groupBy(_.document))
      yield {
      if (k != null)
        errprint("Processing document %s...", k)
      val results = (for (word <- g) yield return_word(word)).toArray

      // Now compute context for words
      val nbcl = driver.params.naive_bayes_context_len
      if (strategy.need_context()) {
        // First determine whether each word is a stopword
        for (i <- 0 until results.length) {
          // FIXME: Check that we aren't accessing a list or something with
          // O(N) random access
          // If a word tagged as a toponym is homonymous with a stopword, it
          // still isn't a stopword.
          results(i).is_stop = (results(i).coord == null &&
            (driver.stopwords contains results(i).word))
        }
        // Now generate context for toponyms
        for (i <- 0 until results.length) {
          // FIXME: Check that we aren't accessing a list or something with
          // O(N) random access
          if (results(i).coord != null) {
            // Select up to naive_bayes_context_len words on either side;
            // skip stopwords.  Associate each word with the distance away from
            // the toponym.
            val minind = 0 max i - nbcl
            val maxind = results.length min i + nbcl + 1
            results(i).context =
              (for {
                (dist, x) <- ((i - minind until i - maxind) zip results.slice(minind, maxind))
                if (!(driver.stopwords contains x.word))
              } yield (dist, x.word)).toArray
          }
        }
      }

      val geogwords =
        (for (word <- results if word.coord != null) yield word).toIterable
      new GeogWordDocument(geogwords)
    }
  }

  // Disambiguate the toponym, specified in GEOGWORD.  Determine the possible
  // locations that the toponym can map to, and call COMPUTE_SCORE on each one
  // to determine a score.  The best score determines the location considered
  // "correct".  Locations without a matching document are skipped.  The
  // location considered "correct" is compared with the actual correct
  // location specified in the toponym, and global variables corresponding to
  // the total number of toponyms processed and number correctly determined are
  // incremented.  Various debugging info is output if 'debug' is set.
  // COMPUTE_SCORE is passed two arguments: GEOGWORD and the location to
  // compute the score of.

  def disambiguate_toponym(geogword: GeogWord) {
    val toponym = geogword.word
    val coord = geogword.coord
    if (coord == null) return // If no ground-truth, skip it
    val documents =
      driver.document_table.wikipedia_subtable.construct_candidates(toponym)
    var bestscore = Double.MinValue
    var bestdoc: TopoDocument = null
    if (documents.length == 0) {
      if (debug("some"))
        errprint("Unable to find any possibilities for %s", toponym)
    } else {
      if (debug("some")) {
        errprint("Considering toponym %s, coordinates %s",
          toponym, coord)
        errprint("For toponym %s, %d possible documents",
          toponym, documents.length)
      }
      for (idoc <- documents) {
        val doc = idoc.asInstanceOf[TopoDocument]
        if (debug("some"))
          errprint("Considering document %s", doc)
        val thisscore = strategy.compute_score(geogword, doc)
        if (thisscore > bestscore) {
          bestscore = thisscore
          bestdoc = doc
        }
      }
    }
    val correct =
      if (bestdoc != null)
        bestdoc.matches_coord(coord)
      else
        false

    val num_candidates = documents.length

    val reason =
      if (correct) null
      else {
        if (num_candidates == 0)
          "incorrect_with_no_candidates"
        else {
          val good_docs =
            (for { idoc <- documents
                   val doc = idoc.asInstanceOf[TopoDocument]
                   if doc.matches_coord(coord)
                 }
             yield doc)
          if (good_docs == null)
            "incorrect_with_no_correct_candidates"
          else if (good_docs.length > 1)
            "incorrect_with_multiple_correct_candidates"
          else {
            val gooddoc = good_docs(0)
            if (gooddoc.incoming_links == None)
              "incorrect_one_correct_candidate_missing_link_info"
            else
              "incorrect_one_correct_candidate"
          }
        }
      }

    errout("Eval: Toponym %s (true: %s at %s),", toponym, geogword.location,
      coord)
    if (correct)
      errprint("correct")
    else
      errprint("incorrect, reason = %s", reason)

    toponym_results.record_geolocate_toponym_result(correct, toponym,
      geogword.location, reason, num_candidates)

    if (debug("some") && bestdoc != null) {
      errprint("Best document = %s, score = %s, dist = %s, correct %s",
        bestdoc, bestscore, bestdoc.distance_to_coord(coord), correct)
    }
  }

  def evaluate_document(doc: GeogWordDocument, doctag: String) = {
    for (geogword <- doc.words)
      disambiguate_toponym(geogword)
    new ToponymEvaluationResult()
  }

  def output_results(isfinal: Boolean = false) {
    toponym_results.output_geolocate_toponym_results()
  }
}

class TRCoNLLGeolocateToponymEvaluator(
  strategy: GeolocateToponymStrategy,
  stratname: String,
  driver: GeolocateToponymDriver
) extends GeolocateToponymEvaluator(strategy, stratname, driver) {
  // Read a file formatted in TR-CONLL text format (.tr files).  An example of
  // how such files are fomatted is:
  //
  //...
  //...
  //last    O       I-NP    JJ
  //week    O       I-NP    NN
  //&equo;s O       B-NP    POS
  //U.N.    I-ORG   I-NP    NNP
  //Security        I-ORG   I-NP    NNP
  //Council I-ORG   I-NP    NNP
  //resolution      O       I-NP    NN
  //threatening     O       I-VP    VBG
  //a       O       I-NP    DT
  //ban     O       I-NP    NN
  //on      O       I-PP    IN
  //Sudanese        I-MISC  I-NP    NNP
  //flights O       I-NP    NNS
  //abroad  O       I-ADVP  RB
  //if      O       I-SBAR  IN
  //Khartoum        LOC
  //        >c1     NGA     15.5833333      32.5333333      Khartoum > Al Khar<BA>om > Sudan
  //        c2      NGA     -17.8833333     30.1166667      Khartoum > Zimbabwe
  //        c3      NGA     15.5880556      32.5341667      Khartoum > Al Khar<BA>om > Sudan
  //        c4      NGA     15.75   32.5    Khartoum > Al Khar<BA>om > Sudan
  //does    O       I-VP    VBZ
  //not     O       I-NP    RB
  //hand    O       I-NP    NN
  //over    O       I-PP    IN
  //three   O       I-NP    CD
  //men     O       I-NP    NNS
  //...
  //...
  //
  // Yield GeogWord objects, one per word.
  def iter_geogwords(filehand: FileHandler, filename: String) = {
    var in_loc = false
    var wordstruct: GeogWord = null
    val lines = filehand.openr(filename, errors = "replace")
    def iter_1(): Stream[GeogWord] = {
      if (lines.hasNext) {
        val line = lines.next
        try {
          val ss = """\t""".r.split(line)
          require(ss.length == 2)
          val Array(word, ty) = ss
          if (word != null) {
            var toyield = null: GeogWord
            if (in_loc) {
              in_loc = false
              toyield = wordstruct
            }
            wordstruct = new GeogWord(word)
            wordstruct.document = filename
            if (ty.startsWith("LOC")) {
              in_loc = true
              wordstruct.is_toponym = true
            } else
              toyield = wordstruct
            if (toyield != null)
              return toyield #:: iter_1()
          } else if (in_loc && ty(0) == '>') {
            val ss = """\t""".r.split(ty)
            require(ss.length == 5)
            val Array(_, lat, long, fulltop, _) = ss
            wordstruct.coord = SphereCoord(lat.toDouble, long.toDouble)
            wordstruct.location = fulltop
          }
        } catch {
          case exc: Exception => {
            errprint("Bad line %s", line)
            errprint("Exception is %s", exc)
            if (!exc.isInstanceOf[NumberFormatException])
              exc.printStackTrace()
          }
        }
        return iter_1()
      } else if (in_loc)
        return wordstruct #:: Stream[GeogWord]()
      else
        return Stream[GeogWord]()
    }
    new GeogWordDocument(iter_1())
  }
}

class WikipediaGeolocateToponymEvaluator(
  strategy: GeolocateToponymStrategy,
  stratname: String,
  driver: GeolocateToponymDriver
) extends GeolocateToponymEvaluator(strategy, stratname, driver) {
  def iter_geogwords(filehand: FileHandler, filename: String) = {
    var title: String = null
    val titlere = """Article title: (.*)$""".r
    val linkre = """Link: (.*)$""".r
    val lines = filehand.openr(filename, errors = "replace")
    def iter_1(): Stream[GeogWord] = {
      if (lines.hasNext) {
        val line = lines.next
        line match {
          case titlere(mtitle) => {
            title = mtitle
            iter_1()
          }
          case linkre(mlink) => {
            val args = mlink.split('|')
            val truedoc = args(0)
            var linkword = truedoc
            if (args.length > 1)
              linkword = args(1)
            val word = new GeogWord(linkword)
            word.is_toponym = true
            word.location = truedoc
            word.document = title
            val doc =
              driver.document_table.wikipedia_subtable.
                lookup_document(truedoc)
            if (doc != null)
              word.coord = doc.coord
            word #:: iter_1()
          }
          case _ => {
            val word = new GeogWord(line)
            word.document = title
            word #:: iter_1()
          }
        }
      } else
        Stream[GeogWord]()
    }
    new GeogWordDocument(iter_1())
  }
}

class Gazetteer(val cell_grid: TopoCellGrid) {

  // Factory object for creating new divisions relative to the gazetteer
  val divfactory = new DivisionFactory(this)

  // For each toponym (name of location), value is a list of Locality
  // items, listing gazetteer locations and corresponding matching documents.
  val lower_toponym_to_location = bufmap[String,Locality]()

  // For each toponym corresponding to a division higher than a locality,
  // list of divisions with this name.
  val lower_toponym_to_division = bufmap[String,Division]()

  // Table of all toponyms seen in evaluation files, along with how many
  // times seen.  Used to determine when caching of certain
  // toponym-specific values should be done.
  //val toponyms_seen_in_eval_files = intmap[String]()

  /**
   * Record mapping from name to Division.
   */
  def record_division(name: String, div: Division) {
    lower_toponym_to_division(name) += div
  }

  // Given an evaluation file, count the toponyms seen and add to the
  // global count in toponyms_seen_in_eval_files.
  //  def count_toponyms_in_file(fname: String) {
  //    def count_toponyms(geogword: GeogWord) {
  //      toponyms_seen_in_eval_files(geogword.word.toLowerCase) += 1
  //    }
  //    process_eval_file(fname, count_toponyms, compute_context = false,
  //                      only_toponyms = true)
  //  }
}

/**
 * Gazetteer of the World-gazetteer format.
 *
 * @param filename File holding the World Gazetteer.
 *
 * @param cell_grid FIXME: Currently required for certain internal reasons.
 *   Fix so we don't need it, or it's created internally!
 */
class WorldGazetteer(
  filehand: FileHandler,
  filename: String,
  cell_grid: TopoCellGrid
) extends Gazetteer(cell_grid) {

  // Find the document matching an entry in the gazetteer.
  // The format of an entry is
  //
  // ID  NAME  ALTNAMES  ORIG-SCRIPT-NAME  TYPE  POPULATION  LAT  LONG  DIV1  DIV2  DIV3
  //
  // where there is a tab character separating each field.  Fields may
  // be empty; but there will still be a tab character separating the
  // field from others.
  //
  // The ALTNAMES specify any alternative names of the location, often
  // including the equivalent of the original name without any accent
  // characters.  If there is more than one alternative name, the
  // possibilities are separated by a comma and a space, e.g.
  // "Dongshi, Dongshih, Tungshih".  The ORIG-SCRIPT-NAME is the name
  // in its original script, if that script is not Latin characters
  // (e.g. names in Russia will be in Cyrillic). (For some reason, names
  // in Chinese characters are listed in the ALTNAMES rather than the
  // ORIG-SCRIPT-NAME.)
  //
  // LAT and LONG specify the latitude and longitude, respectively.
  // These are given as integer values, where the actual value is found
  // by dividing this integer value by 100.
  //
  // DIV1, DIV2 and DIV3 specify different-level divisions that a location is
  // within, from largest to smallest.  Typically the largest is a country.
  // For locations in the U.S., the next two levels will be state and county,
  // respectively.  Note that such divisions also have corresponding entries
  // in the gazetteer.  However, these entries are somewhat lacking in that
  // (1) no coordinates are given, and (2) only the top-level division (the
  // country) is given, even for third-level divisions (e.g. counties in the
  // U.S.).
  //
  // For localities, add them to the cell-map that covers the earth if
  // ADD_TO_CELL_MAP is true.

  protected def match_world_gazetteer_entry(line: String) {
    // Split on tabs, make sure at least 11 fields present and strip off
    // extra whitespace
    var fields = """\t""".r.split(line.trim) ++ Seq.fill(11)("")
    fields = (for (x <- fields.slice(0, 11)) yield x.trim)
    val Array(id, name, altnames, orig_script_name, typ, population,
      lat, long, div1, div2, div3) = fields

    // Skip places without coordinates
    if (lat == "" || long == "") {
      if (debug("lots"))
        errprint("Skipping location %s (div %s/%s/%s) without coordinates",
          name, div1, div2, div3)
      return
    }

    if (lat == "0" && long == "9999") {
      if (debug("lots"))
        errprint("Skipping location %s (div %s/%s/%s) with bad coordinates",
          name, div1, div2, div3)
      return
    }

    // Create and populate a Locality object
    val loc = new Locality(name, SphereCoord(lat.toInt / 100., long.toInt / 100.),
      typ = typ, altnames = if (altnames != null) ", ".r.split(altnames) else null)
    loc.div = divfactory.find_division_note_point(loc, Seq(div1, div2, div3))
    if (debug("lots"))
      errprint("Saw location %s (div %s/%s/%s) with coordinates %s",
        loc.name, div1, div2, div3, loc.coord)

    // Record the location.  For each name for the location (its
    // canonical name and all alternates), add the location to the list of
    // locations associated with the name.  Record the name in lowercase
    // for ease in matching.
    for (name <- Seq(loc.name) ++ loc.altnames) {
      val loname = name.toLowerCase
      if (debug("lots"))
        errprint("Noting lower_toponym_to_location for toponym %s, canonical name %s",
                 name, loc.name)
      lower_toponym_to_location(loname) += loc
    }

    // We start out looking for documents whose distance is very close,
    // then widen until we reach params.max_dist_for_close_match.
    var maxdist = 5
    var docmatch: TopoDocument = null
    val topo_table = cell_grid.table.asInstanceOf[TopoDocumentTable]
    val params = topo_table.topo_driver.params

    breakable {
      while (maxdist <= params.max_dist_for_close_match) {
        docmatch =
          topo_table.topo_subtable.find_match_for_locality(loc, maxdist)
        if (docmatch != null) break
        maxdist *= 2
      }
    }

    if (docmatch == null) {
      if (debug("lots"))
        errprint("Unmatched name %s", loc.name)
      return
    }

    // Record the match.
    loc.docmatch = docmatch
    docmatch.location = loc
    if (debug("lots"))
      errprint("Matched location %s (coord %s) with document %s, dist=%s",
        (loc.name, loc.coord, docmatch,
          spheredist(loc.coord, docmatch.coord)))
  }

  // Read in the data from the World gazetteer in FILENAME and find the
  // document matching each entry in the gazetteer.  (Unimplemented:
  // For localities, add them to the cell-map that covers the earth if
  // ADD_TO_CELL_MAP is true.)
  protected def read_world_gazetteer_and_match() {
    val topo_table = cell_grid.table.asInstanceOf[TopoDocumentTable]
    val params = topo_table.topo_driver.params

    val task = new ExperimentMeteredTask(cell_grid.table.driver,
      "gazetteer entry", "matching", maxtime = params.max_time_per_stage)
    errprint("Matching gazetteer entries in %s...", filename)
    errprint("")

    // Match each entry in the gazetteer
    breakable {
      val lines = filehand.openr(filename)
      try {
        for (line <- lines) {
          if (debug("lots"))
            errprint("Processing line: %s", line)
          match_world_gazetteer_entry(line)
          if (task.item_processed())
            break
        }
      } finally {
        lines.close()
      }
    }

    divfactory.finish_all()
    task.finish()
    output_resource_usage()
  }

  // Upon creation, populate gazetteer from file
  read_world_gazetteer_and_match()
}

class GeolocateToponymParameters(
  parser: ArgParser = null
) extends GeolocateParameters(parser) {
  var eval_format =
    ap.option[String]("f", "eval-format",
      default = "wikipedia",
      choices = Seq("wikipedia", "raw-text", "tr-conll"),
      help = """Format of evaluation file(s).  The evaluation files themselves
are specified using --eval-file.  The following formats are
recognized:

'wikipedia' is the normal format.  The data file is in a format very
similar to that of the counts file, but has "toponyms" identified using
the prefix 'Link: ' followed either by a toponym name or the format
'DOCUMENT-NAME|TOPONYM', indicating a toponym (e.g. 'London') that maps
to a given document that disambiguates the toponym (e.g. 'London,
Ontario').  When a raw toponym is given, the document is assumed to have
the same name as the toponym. (The format is called 'wikipedia' because
the link format used here comes directly from the way that links are
specified in Wikipedia documents, and often the text itself comes from
preprocessing a Wikipedia dump.) The mapping here is used for evaluation
but not for constructing training data.

'raw-text' assumes that the eval file is simply raw text.
(NOT YET IMPLEMENTED.)

'tr-conll' is an alternative.  It specifies the toponyms in a document
along with possible locations to map to, with the correct one identified.
As with the 'document' format, the correct location is used only for
evaluation, not for constructing training data; the other locations are
ignored.""")

  var gazetteer_file =
    ap.option[String]("gazetteer-file", "gf",
      help = """File containing gazetteer information to match.""")
  var gazetteer_type =
    ap.option[String]("gazetteer-type", "gt",
      metavar = "FILE",
      default = "world", choices = Seq("world", "db"),
      help = """Type of gazetteer file specified using --gazetteer-file.
NOTE: type 'world' is the only one currently implemented.  Default
'%default'.""")

  var strategy =
    ap.multiOption[String]("s", "strategy",
      default = Seq("baseline"),
      //      choices = Seq(
      //        "baseline", "none",
      //        "naive-bayes-with-baseline",
      //        "naive-bayes-no-baseline",
      //        ),
      aliases = Map(
        "baseline" -> null, "none" -> null,
        "naive-bayes-with-baseline" ->
          Seq("nb-base"),
        "naive-bayes-no-baseline" ->
          Seq("nb-nobase")),
      help = """Strategy/strategies to use for geolocating.
'baseline' means just use the baseline strategy (see --baseline-strategy).

'none' means don't do any geolocating.  Useful for testing the parts that
read in data and generate internal structures.

'naive-bayes-with-baseline' (or 'nb-base') means also use the words around the
toponym to be disambiguated, in a Naive-Bayes scheme, using the baseline as the
prior probability; 'naive-bayes-no-baseline' (or 'nb-nobase') means use uniform
prior probability.

Default is 'baseline'.

NOTE: Multiple --strategy options can be given, and each strategy will
be tried, one after the other.""")

  var baseline_strategy =
    ap.multiOption[String]("baseline-strategy", "bs",
      default = Seq("internal-link"),
      choices = Seq("internal-link", "random",
        "num-documents"),
      aliases = Map(
        "internal-link" -> Seq("link"),
        "num-documents" -> Seq("num-docs", "numdocs")),
      help = """Strategy to use to compute the baseline.

'internal-link' (or 'link') means use number of internal links pointing to the
document or cell.

'random' means choose randomly.

'num-documents' (or 'num-docs' or 'numdocs'; only in cell-type matching) means
use number of documents in cell.

Default '%default'.

NOTE: Multiple --baseline-strategy options can be given, and each strategy will
be tried, one after the other.""")

  var naive_bayes_context_len =
    ap.option[Int]("naive-bayes-context-len", "nbcl",
      default = 10,
      help = """Number of words on either side of a toponym to use
in Naive Bayes matching, during toponym resolution.  Default %default.""")
  var max_dist_for_close_match =
    ap.option[Double]("max-dist-for-close-match", "mdcm",
      default = 80.0,
      help = """Maximum number of km allowed when looking for a
close match for a toponym during toponym resolution.  Default %default.""")
  var max_dist_for_outliers =
    ap.option[Double]("max-dist-for-outliers", "mdo",
      default = 200.0,
      help = """Maximum number of km allowed between a point and
any others in a division, during toponym resolution.  Points farther away than
this are ignored as "outliers" (possible errors, etc.).  NOTE: Not
currently implemented. Default %default.""")
  var context_type =
    ap.option[String]("context-type", "ct",
      default = "cell-dist-document-links",
      choices = Seq("document", "cell", "cell-dist-document-links"),
      help = """Type of context used when doing disambiguation.
There are two cases where this choice applies: When computing a word
distribution, and when counting the number of incoming internal links.
'document' means use the document itself for both.  'cell' means use the
cell for both. 'cell-dist-document-links' means use the cell for
computing a word distribution, but the document for counting the number of
incoming internal links. (Note that this only applies when doing toponym
resolution.  During document resolution, only cells are considered.)
Default '%default'.""")
}

class GeolocateToponymDriver extends
    GeolocateDriver with StandaloneExperimentDriverStats {
  type TParam = GeolocateToponymParameters
  type TRunRes =
    Seq[(String, GeolocateToponymStrategy, CorpusEvaluator[_,_])]

  override def handle_parameters() {
    super.handle_parameters()

    /* FIXME: Eliminate this. */
    GeolocateToponymApp.Params = params
    
    need(params.gazetteer_file, "gazetteer-file")
    // FIXME! Can only currently handle World-type gazetteers.
    if (params.gazetteer_type != "world")
      param_error("Currently can only handle world-type gazetteers")

    if (params.strategy == Seq("baseline"))
      ()

    if (params.eval_format == "raw-text") {
      // FIXME!!!!
      param_error("Raw-text reading not implemented yet")
    }

    need_seq(params.eval_file, "eval-file", "evaluation file(s)")
  }

  override protected def initialize_document_table(
      word_dist_factory: WordDistFactory) = {
    new TopoDocumentTable(this, word_dist_factory)
  }

  override protected def initialize_cell_grid(table: SphereDocumentTable) = {
    if (params.kd_tree)
      param_error("Can't currently handle K-d trees")
    new TopoCellGrid(degrees_per_cell, params.width_of_multi_cell,
      table.asInstanceOf[TopoDocumentTable])
  }

  /**
   * Do the actual toponym geolocation.  Results to stderr (see above), and
   * also returned.
   *
   * Return value very much like for run_after_setup() for document
   * geolocation, but less useful info may be returned for each document
   * processed.
   */

  def run_after_setup() = {
    // errprint("Processing evaluation file(s) %s for toponym counts...",
    //   args.eval_file)
    // process_dir_files(args.eval_file, count_toponyms_in_file)
    // errprint("Number of toponyms seen: %s",
    //   toponyms_seen_in_eval_files.length)
    // errprint("Number of toponyms seen more than once: %s",
    //   (for {(foo,count) <- toponyms_seen_in_eval_files
    //             if (count > 1)} yield foo).length)
    // output_reverse_sorted_table(toponyms_seen_in_eval_files,
    //                             outfile = sys.stderr)

    if (params.gazetteer_file != null) {
      /* FIXME!!! */
      assert(cell_grid.isInstanceOf[TopoCellGrid])
      val gazetteer =
        new WorldGazetteer(get_file_handler, params.gazetteer_file,
          cell_grid.asInstanceOf[TopoCellGrid])
      // Bootstrapping issue: Creating the gazetteer requires that the
      // TopoDocumentTable already exist, but the TopoDocumentTable wants
      // a pointer to a gazetter, so have to set it afterwards.
      document_table.wikipedia_subtable.
        asInstanceOf[TopoDocumentSubtable].set_gazetteer(gazetteer)
    }

    val strats_unflat = (
      for (stratname <- params.strategy) yield {
        // Generate strategy object
        if (stratname == "baseline") {
          for (basestratname <- params.baseline_strategy)
            yield ("baseline " + basestratname,
            new BaselineGeolocateToponymStrategy(cell_grid, basestratname))
        } else {
          val strategy = new NaiveBayesToponymStrategy(cell_grid,
            use_baseline = (stratname == "naive-bayes-with-baseline"))
          Seq((stratname, strategy))
        }
      })
    val strats = strats_unflat reduce (_ ++ _)
    process_strategies(strats)((stratname, strategy) => {
      // Generate reader object
      if (params.eval_format == "tr-conll")
        new TRCoNLLGeolocateToponymEvaluator(strategy, stratname, this)
      else
        new WikipediaGeolocateToponymEvaluator(strategy, stratname, this)
    })
  }
}

object GeolocateToponymApp extends GeolocateApp("geolocate-toponyms") {
  type TDriver = GeolocateToponymDriver
  // FUCKING TYPE ERASURE
  def create_param_object(ap: ArgParser) = new TParam(ap)
  def create_driver() = new TDriver()
  var Params: TParam = _
}
