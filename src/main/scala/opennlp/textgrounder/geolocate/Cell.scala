
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

////////
//////// Cell.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate

import math._
import collection.mutable

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.distances._
import opennlp.textgrounder.util.MeteredTask
import opennlp.textgrounder.util.ioutil._

import WordDist.memoizer._
import GeolocateDriver.Args
import GeolocateDriver.Debug._

/////////////////////////////////////////////////////////////////////////////
//                             Word distributions                          //
/////////////////////////////////////////////////////////////////////////////

/**
 * Distribution over words corresponding to a cell.
 */

class CellWordDist(val word_dist: WordDist) {
  /** Number of articles included in incoming-link computation. */
  var num_arts_for_links = 0
  /** Total number of incoming links. */
  var incoming_links = 0
  /** Number of articles included in word distribution. */
  var num_arts_for_word_dist = 0

  def is_empty_for_word_dist() = num_arts_for_word_dist == 0

  def is_empty() = num_arts_for_links == 0

  /**
   *  Add the given article to the total distribution seen so far
   */
  def add_article(art: GeoArticle) {
    /* We are passed in all articles, regardless of the split.
       The decision was made to accumulate link counts from all articles,
       even in the evaluation set.  Strictly, this is a violation of the
       "don't train on your evaluation set" rule.  The reason we do this
       is that

       (1) The links are used only in Naive Bayes, and only in establishing
       a prior probability.  Hence they aren't the main indicator.
       (2) Often, nearly all the link count for a given cell comes from
       a particular article -- e.g. the Wikipedia article for the primary
       city in the cell.  If we pull the link count for this article
       out of the cell because it happens to be in the evaluation set,
       we will totally distort the link count for this cell.  In a "real"
       usage case, we would be testing against an unknown article, not
       against an article in our training set that we've artificially
       removed so as to construct an evaluation set, and this problem
       wouldn't arise, so by doing this we are doing a more realistic
       evaluation.
       
       Note that we do NOT include word counts from dev-set or test-set
       articles in the word distribution for a cell.  This keeps to the
       above rule about only training on your training set, and is OK
       because (1) each article in a cell contributes a similar amount of
       word counts (assuming the articles are somewhat similar in size),
       hence in a cell with multiple articles, each individual article
       only computes a fairly small fraction of the total word counts;
       (2) distributions are normalized in any case, so the exact number
       of articles in a cell does not affect the distribution. */
    /* Add link count of article to cell. */
    art.incoming_links match {
      // Might be None, for unknown link count
      case Some(x) => incoming_links += x
      case _ =>
    }
    num_arts_for_links += 1

    /* Add word counts of article to cell, but only if in the
       training set. */
    if (art.split == "training") {
      if (art.dist == null) {
        if (Args.max_time_per_stage == 0.0 && Args.num_training_docs == 0)
          warning("Saw article %s without distribution", art)
      } else {
        assert(art.dist.finished)
        word_dist.add_word_distribution(art.dist)
        num_arts_for_word_dist += 1
      }
    }
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Cell distributions                          //
/////////////////////////////////////////////////////////////////////////////

/** A simple distribution associating a probability with each cell. */

class CellDist(
  val cellprobs: mutable.Map[GeoCell, Double]) {
  def get_ranked_cells() = {
    // sort by second element of tuple, in reverse order
    cellprobs.toSeq sortWith (_._2 > _._2)
  }
}

/**
 * Distribution over cells, as might be attached to a word.  If we have a
 *  set of cells, each with a word distribution, then we can imagine
 *  conceptually inverting the process to generate a cell distribution over
 *  words.  Basically, for a given word, look to see what its probability is
 *  in all cells; normalize, and we have a cell distribution.
 *
 *  @param word Word for which the cell is computed
 *  @param cellprobs Hash table listing probabilities associated with cells
 */

class WordCellDist(
  val cell_grid: CellGrid,
  val word: Word
) extends CellDist(mutable.Map[GeoCell, Double]()) {
  var normalized = false

  protected def init() {
    // It's expensive to compute the value for a given word so we cache word
    // distributions.
    var totalprob = 0.0
    // Compute and store un-normalized probabilities for all cells
    for (cell <- cell_grid.iter_nonempty_cells(nonempty_word_dist = true)) {
      val prob = cell.word_dist.lookup_word(word)
      // Another way of handling zero probabilities.
      /// Zero probabilities are just a bad idea.  They lead to all sorts of
      /// pathologies when trying to do things like "normalize".
      //if (prob == 0.0)
      //  prob = 1e-50
      cellprobs(cell) = prob
      totalprob += prob
    }
    // Normalize the probabilities; but if all probabilities are 0, then
    // we can't normalize, so leave as-is. (FIXME When can this happen?
    // It does happen when you use --mode=generate-kml and specify words
    // that aren't seen.  In other circumstances, the smoothing ought to
    // ensure that 0 probabilities don't exist?  Anything else I missed?)
    if (totalprob != 0) {
      normalized = true
      for ((cell, prob) <- cellprobs)
        cellprobs(cell) /= totalprob
    } else
      normalized = false
  }

  init()

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
        kml <- cell.generate_kml(xform(prob), xf_minprob, xf_maxprob, params)
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
            <name>{ unmemoize_word(word) }</name>
            <open>1</open>
            <description>{ "Cell distribution for word '%s'" format unmemoize_word(word) }</description>
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

class CellDistFactory(val lru_cache_size: Int) {
  var cached_dists: LRUCache[Word, WordCellDist] = null

  // Return a cell distribution over a given word, using a least-recently-used
  // cache to optimize access.
  def get_cell_dist(cell_grid: CellGrid, word: Word) = {
    if (cached_dists == null)
      cached_dists = new LRUCache(maxsize = lru_cache_size)
    cached_dists.get(word) match {
      case Some(dist) => dist
      case None => {
        val dist = new WordCellDist(cell_grid, word)
        cached_dists(word) = dist
        dist
      }
    }
  }

  /**
   * Return a cell distribution over a distribution over words.  This works
   * by adding up the distributions of the individual words, weighting by
   * the count of the each word.
   */
  def get_cell_dist_for_word_dist(cell_grid: CellGrid, xword_dist: WordDist) = {
    // FIXME!!! Figure out what to do if distribution is not a unigram dist.
    // Can we break this up into smaller operations?  Or do we have to
    // make it an interface for WordDist?
    val word_dist = xword_dist.asInstanceOf[UnigramWordDist]
    val cellprobs = doublemap[GeoCell]()
    for ((word, count) <- word_dist.counts) {
      val dist = get_cell_dist(cell_grid, word)
      for ((cell, prob) <- dist.cellprobs)
        cellprobs(cell) += count * prob
    }
    val totalprob = (cellprobs.values sum)
    for ((cell, prob) <- cellprobs)
      cellprobs(cell) /= totalprob
    new CellDist(cellprobs)
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Cells in a grid                             //
/////////////////////////////////////////////////////////////////////////////

/**
 * Abstract class for a general cell in a cell grid.
 * 
 * @param cell_grid The CellGrid object for the grid this cell is in.
 */
abstract class GeoCell(val cell_grid: CellGrid) {
  val word_dist_wrapper =
    new CellWordDist(cell_grid.table.word_dist_factory.create_word_dist())
  var most_popular_article: GeoArticle = null
  var mostpopart_links = 0

  def word_dist = word_dist_wrapper.word_dist

  /**
   * Return a string describing the location of the cell in its grid,
   * e.g. by its boundaries or similar.
   */
  def describe_location(): String

  /**
   * Return a string describing the indices of the cell in its grid.
   * Only used for debugging.
   */
  def describe_indices(): String

  /**
   * Return an Iterable over articles, listing the articles in the cell.
   */
  def iterate_articles(): Iterable[GeoArticle]

  /**
   * Return the coordinate of the "center" of the cell.  This is the
   * coordinate used in computing distances between arbitary points and
   * given cells, for evaluation and such.  For odd-shaped cells, the
   * center can be more or less arbitrarily placed as long as it's somewhere
   * central.
   */
  def get_center_coord(): Coord

  /**
   * Generate KML for a single cell.
   */
  def generate_kml(xfprob: Double, xf_minprob: Double, xf_maxprob: Double,
    params: KMLParameters): Iterable[xml.Elem]

  /**
   * Return a string representation of the cell.  Generally does not need
   * to be overridden.
   */
  override def toString() = {
    val unfinished = if (word_dist.finished) "" else ", unfinished"
    val contains =
      if (most_popular_article != null)
        ", most-pop-art %s(%d links)" format (
          most_popular_article, mostpopart_links)
      else ""

    "GeoCell(%s%s%s, %d articles(dist), %d articles(links), %d links)" format (
      describe_location(), unfinished, contains,
      word_dist_wrapper.num_arts_for_word_dist,
      word_dist_wrapper.num_arts_for_links,
      word_dist_wrapper.incoming_links)
  }

  // def __repr__() = {
  //   toString.encode("utf-8")
  // }

  /**
   * Return a shorter string representation of the cell, for
   * logging purposes.
   */
  def shortstr() = {
    var str = "Cell %s" format describe_location()
    val mostpop = most_popular_article
    if (mostpop != null)
      str += ", most-popular %s" format mostpop.shortstr()
    str
  }

  /**
   * Return an XML representation of the cell.  Currently used only for
   * debugging-output purposes, so the exact representation isn't too important.
   */
  def struct() =
    <GeoCell>
      <bounds>{ describe_location() }</bounds>
      <finished>{ word_dist.finished }</finished>
      {
        if (most_popular_article != null)
          (<mostPopularArticle>most_popular_article.struct()</mostPopularArticle>
           <mostPopularArticleLinks>mostpopart_links</mostPopularArticleLinks>)
      }
      <numArticlesDist>{ word_dist_wrapper.num_arts_for_word_dist }</numArticlesDist>
      <numArticlesLink>{ word_dist_wrapper.num_arts_for_links }</numArticlesLink>
      <incomingLinks>{ word_dist_wrapper.incoming_links }</incomingLinks>
    </GeoCell>

  /**
   * Generate the distribution for a cell from the articles in it.
   */
  def generate_dist() {
    for (art <- iterate_articles()) {
      word_dist_wrapper.add_article(art)
      if (art.incoming_links != None &&
        art.incoming_links.get > mostpopart_links) {
        mostpopart_links = art.incoming_links.get
        most_popular_article = art
      }
    }
    word_dist.finish(minimum_word_count = Args.minimum_word_count)
  }
}

/**
 * A cell in a polygonal shape.
 *
 * @param cell_grid The CellGrid object for the grid this cell is in.
 */
abstract class PolygonalCell(
  cell_grid: CellGrid) extends GeoCell(cell_grid) {
  /**
   * Return the boundary of the cell as an Iterable of coordinates, tracing
   * out the boundary vertex by vertex.  The last coordinate should be the
   * same as the first, as befits a closed shape.
   */
  def get_boundary(): Iterable[Coord]

  /**
   * Return the "inner boundary" -- something echoing the actual boundary of the
   * cell but with smaller dimensions.  Used for outputting KML to make the
   * output easier to read.
   */
  def get_inner_boundary() = {
    val center = get_center_coord()
    for (coord <- get_boundary())
      yield Coord((center.lat + coord.lat) / 2.0,
                  average_longitudes(center.long, coord.long))
  }

  /**
   * Generate the KML placemark for the cell's name.  Currently it's rectangular
   * for rectangular cells.  FIXME: Perhaps it should be generalized so it doesn't
   * need to be redefined for differently-shaped cells.
   *
   * @param name The name to display in the placemark
   */
  def generate_kml_name_placemark(name: String): xml.Elem

  def generate_kml(xfprob: Double, xf_minprob: Double, xf_maxprob: Double,
      params: KMLParameters) = {
    val offprob = xfprob - xf_minprob
    val fracprob = offprob / (xf_maxprob - xf_minprob)
    var coordtext = "\n"
    for (coord <- get_inner_boundary()) {
      coordtext += "%s,%s,%s\n" format (
        coord.long, coord.lat, fracprob * params.kml_max_height)
    }
    val name =
      if (most_popular_article != null) most_popular_article.title
      else ""

    // Placemark indicating name
    val name_placemark = generate_kml_name_placemark(name)

    // Interpolate colors
    val color = Array(0.0, 0.0, 0.0)
    for (i <- 0 until 3) {
      color(i) = (params.kml_mincolor(i) +
        fracprob * (params.kml_maxcolor(i) - params.kml_mincolor(i)))
    }
    // Original color dc0155ff
    //rgbcolor = "dc0155ff"
    val revcol = color.reverse
    val rgbcolor = "ff%02x%02x%02x" format (
      revcol(0).toInt, revcol(1).toInt, revcol(2).toInt)

    // Yield cylinder indicating probability by height and color

    // !!PY2SCALA: BEGIN_PASSTHRU
    val cylinder_placemark =
      <Placemark>
        <name>{ "%s POLYGON" format name }</name>
        <styleUrl>#bar</styleUrl>
        <Style>
          <PolyStyle>
            <color>{ rgbcolor }</color>
            <colorMode>normal</colorMode>
          </PolyStyle>
        </Style>
        <Polygon>
          <extrude>1</extrude>
          <tessellate>1</tessellate>
          <altitudeMode>relativeToGround</altitudeMode>
          <outerBoundaryIs>
            <LinearRing>
              <coordinates>{ coordtext }</coordinates>
            </LinearRing>
          </outerBoundaryIs>
        </Polygon>
      </Placemark>
    // !!PY2SCALA: END_PASSTHRU
    Seq(name_placemark, cylinder_placemark)
  }
}

/**
 * A cell in a rectangular shape.
 *
 * @param cell_grid The CellGrid object for the grid this cell is in.
 */
abstract class RectangularCell(
  cell_grid: CellGrid
) extends PolygonalCell(cell_grid) {
  /**
   * Return the coordinate of the southwest point of the rectangle.
   */
  def get_southwest_coord(): Coord
  /**
   * Return the coordinate of the northeast point of the rectangle.
   */
  def get_northeast_coord(): Coord
  /**
   * Define the center based on the southwest and northeast points.
   */
  def get_center_coord() = {
    val sw = get_southwest_coord()
    val ne = get_northeast_coord()
    Coord((sw.lat + ne.lat) / 2.0, (sw.long + ne.long) / 2.0)
  }

  /**
   * Define the boundary given the specified southwest and northeast
   * points.
   */
  def get_boundary() = {
    val sw = get_southwest_coord()
    val ne = get_northeast_coord()
    val center = get_center_coord()
    val nw = Coord(ne.lat, sw.long)
    val se = Coord(sw.lat, ne.long)
    Seq(sw, nw, ne, se, sw)
  }

  /**
   * Generate the name placemark as a smaller rectangle within the
   * larger rectangle. (FIXME: Currently it is exactly the size of
   * the inner boundary.  Perhaps this should be generalized, so
   * that the definition of this function can be handled up at the
   * polygonal-shaped-cell level.)
   */
  def generate_kml_name_placemark(name: String) = {
    val sw = get_southwest_coord()
    val ne = get_northeast_coord()
    val center = get_center_coord()
    // !!PY2SCALA: BEGIN_PASSTHRU
    // Because it tries to frob the # sign
    <Placemark>
      <name>{ name }</name>
      ,
      <Cell>
        <LatLonAltBox>
          <north>{ ((center.lat + ne.lat) / 2).toString }</north>
          <south>{ ((center.lat + sw.lat) / 2).toString }</south>
          <east>{ ((center.long + ne.long) / 2).toString }</east>
          <west>{ ((center.long + sw.long) / 2).toString }</west>
        </LatLonAltBox>
        <Lod>
          <minLodPixels>16</minLodPixels>
        </Lod>
      </Cell>
      <styleURL>#bar</styleURL>
      <Point>
        <coordinates>{ "%s,%s" format (center.long, center.lat) }</coordinates>
      </Point>
    </Placemark>
    // !!PY2SCALA: END_PASSTHRU
  }
}

/**
 * Abstract class for a grid of cells covering the earth.
 */
abstract class CellGrid(val table: GeoArticleTable) {
  /**
   * Total number of cells in the grid.
   */
  var total_num_cells: Int

  /**
   * Find the correct cell for the given coordinates.  If no such cell
   * exists, return null.
   */
  def find_best_cell_for_coord(coord: Coord): GeoCell

  /**
   * Add the given article to the cell grid.
   */
  def add_article_to_cell(article: GeoArticle): Unit

  /**
   * Generate all non-empty cells.  This will be called once (and only once),
   * after all articles have been added to the cell grid by calling
   * `add_article_to_cell`.  The generation happens internally; but after
   * this, `iter_nonempty_cells` should work properly.  This is not meant
   * to be called externally.
   */
  protected def initialize_cells(): Unit

  /**
   * Iterate over all non-empty cells.
   * 
   * @param nonempty_word_dist If given, returned cells must also have a
   *   non-empty word distribution; otherwise, they just need to have at least
   *   one article in them. (Not all articles have word distributions, esp.
   *   when --max-time-per-stage has been set to a non-zero value so that we
   *   only load some subset of the word distributions for all articles.  But
   *   even when not set, some articles may be listed in the article-data file
   *   but have no corresponding word counts given in the counts file.)
   */
  def iter_nonempty_cells(nonempty_word_dist: Boolean = false): Iterable[GeoCell]
  
  /*********************** Not meant to be overridden *********************/
  
  /* These are simply the sum of the corresponding counts
     `num_arts_for_word_dist` and `num_arts_for_links` of each individual
     cell. */
  var total_num_arts_for_word_dist = 0
  var total_num_arts_for_links = 0
  /* Set once finish() is called. */
  var all_cells_computed = false
  /* Number of non-empty cells. */
  var num_non_empty_cells = 0

  /**
   * This function is called externally to initialize the cells.  It is a
   * wrapper around `initialize_cells()`, which is not meant to be called
   * externally.  Normally this does not need to be overridden.
   */
  def finish() {
    assert(!all_cells_computed)

    initialize_cells()

    all_cells_computed = true

    total_num_arts_for_links = 0
    total_num_arts_for_word_dist = 0
    for (cell <- iter_nonempty_cells()) {
      total_num_arts_for_word_dist +=
        cell.word_dist_wrapper.num_arts_for_word_dist
      total_num_arts_for_links +=
        cell.word_dist_wrapper.num_arts_for_links
    }

    errprint("Number of non-empty cells: %s", num_non_empty_cells)
    errprint("Total number of cells: %s", total_num_cells)
    errprint("Percent non-empty cells: %g",
      num_non_empty_cells.toDouble / total_num_cells)
    val training_arts_with_word_counts =
      table.num_word_count_articles_by_split("training").value
    errprint("Training articles per non-empty cell: %g",
      training_arts_with_word_counts.toDouble / num_non_empty_cells)
    // Clear out the article distributions of the training set, since
    // only needed when computing cells.
    //
    // FIXME: Could perhaps save more memory, or at least total memory used,
    // by never creating these distributions at all, but directly adding
    // them to the cells.  Would require a bit of thinking when reading
    // in the counts.
    table.clear_training_article_distributions()
  }
}

/////////////////////////////////////////////////////////////////////////////
//                         A regularly spaced grid                         //
/////////////////////////////////////////////////////////////////////////////

/* 
  We divide the earth's surface into "tiling cells", all of which are the
  same square size, running on latitude/longitude lines, and which have a
  constant number of degrees on a size, set using the value of the command-
  line option --degrees-per-cell. (Alternatively, the value of --miles-per-cell
  or --km-per-cell are converted into degrees using 'miles_per_degree' or
  'km_per_degree', respectively, which specify the size of a degree at
  the equator and is derived from the value for the Earth's radius.)

  In addition, we form a square of tiling cells in order to create a
  "multi cell", which is used to compute a distribution over words.  The
  number of tiling cells on a side of a multi cell is determined by
  --width-of-multi-cell.  Note that if this is greater than 1, different
  multi cells will overlap.

  To specify a cell, we use cell indices, which are derived from
  coordinates by dividing by degrees_per_cell.  Hence, if for example
  degrees_per_cell is 2.0, then cell indices are in the range [-45,+45]
  for latitude and [-90,+90) for longitude.  In general, an arbitrary
  coordinate will have fractional cell indices; however, the cell indices
  of the corners of a cell (tiling or multi) will be integers.  Normally,
  we use the southwest corner to specify a cell.

  Correspondingly, to convert a cell index to a Coord, we multiply
  latitude and longitude by degrees_per_cell.

  Near the edges, tiling cells may be truncated.  Multi cells will
  wrap around longitudinally, and will still have the same number of
  tiling cells, but may be smaller.
  */

/**
 * The index of a regular cell, using "cell index" integers, as described
 * above.
 */
case class RegularCellIndex(latind: Int, longind: Int) {
  def toFractional() = FractionalRegularCellIndex(latind, longind)
}

/**
 * Similar to `RegularCellIndex`, but for the case where the indices are
 * fractional, representing a location other than at the corners of a
 * cell.
 */
case class FractionalRegularCellIndex(latind: Double, longind: Double) {
}

/**
 * A cell where the cell grid is a MultiRegularCellGrid. (See that class.)
 *
 * @param cell_grid The CellGrid object for the grid this cell is in,
 *   an instance of MultiRegularCellGrid.
 * @param index Index of this cell in the grid
 */

class MultiRegularCell(
  cell_grid: MultiRegularCellGrid,
  val index: RegularCellIndex
) extends RectangularCell(cell_grid) {

  def get_southwest_coord() =
    cell_grid.multi_cell_index_to_near_corner_coord(index)

  def get_northeast_coord() =
    cell_grid.multi_cell_index_to_far_corner_coord(index)

  def describe_location() = {
    "%s-%s" format (get_southwest_coord(), get_northeast_coord())
  }

  def describe_indices() = "%s,%s" format (index.latind, index.longind)

  def iterate_articles() = {
    val maxlatind = (
      (cell_grid.maximum_latind + 1) min
      (index.latind + cell_grid.width_of_multi_cell))

    if (debug("lots")) {
      errprint("Generating distribution for multi cell centered at %s",
        cell_grid.cell_index_to_coord(index))
    }

    // Process the tiling cells making up the multi cell;
    // but be careful around the edges.  Truncate the latitude, wrap the
    // longitude.
    for {
      // The use of view() here in both iterators causes this iterable to
      // be lazy; hence the print statement below doesn't get executed until
      // we actually process the articles in question.
      i <- (index.latind until maxlatind) view;
      rawj <- (index.longind until
        (index.longind + cell_grid.width_of_multi_cell)) view;
      val j = (if (rawj > cell_grid.maximum_longind) rawj - 360 else rawj)
      art <- {
        if (debug("lots")) {
          errprint("--> Processing tiling cell %s",
            cell_grid.cell_index_to_coord(index))
        }
        cell_grid.tiling_cell_to_articles.getNoSet(RegularCellIndex(i, j))
      }
    } yield art
  }
}

/**
 * Grid composed of possibly-overlapping multi cells, based on an underlying
 * grid of regularly-spaced square cells tiling the earth.  The multi cells,
 * over which word distributions are computed for comparison with the word
 * distribution of a given article, are composed of NxN tiles, where possibly
 * N > 1.
 *
 * FIXME: We should abstract out the concept of a grid composed of tiles and
 * a grid composed of overlapping conglomerations of tiles; this could be
 * useful e.g. for KD trees or other representations where we might want to
 * compare with cells at multiple levels of granularity.
 * 
 * @param degrees_per_cell Size of each cell in degrees.  Determined by the
 *   --degrees-per-cell option, unless --miles-per-cell is set, in which
 *   case it takes priority.
 * @param width_of_multi_cell Size of multi cells in tiling cells,
 *   determined by the --width-of-multi-cell option.
 */
class MultiRegularCellGrid(
  val degrees_per_cell: Double,
  val width_of_multi_cell: Int,
  table: GeoArticleTable
) extends CellGrid(table) {

  /**
   * Size of each cell (vertical dimension; horizontal dimension only near
   * the equator) in km.  Determined from degrees_per_cell.
   */
  val km_per_cell = degrees_per_cell * km_per_degree

  /* Set minimum, maximum latitude/longitude in indices (integers used to
     index the set of cells that tile the earth).   The actual maximum
     latitude is exactly 90 (the North Pole).  But if we set degrees per
     cell to be a number that exactly divides 180, and we use
     maximum_latitude = 90 in the following computations, then we would
     end up with the North Pole in a cell by itself, something we probably
     don't want.
   */
  val maximum_index =
    coord_to_tiling_cell_index(Coord(maximum_latitude - 1e-10,
      maximum_longitude))
  val maximum_latind = maximum_index.latind
  val maximum_longind = maximum_index.longind
  val minimum_index =
    coord_to_tiling_cell_index(Coord(minimum_latitude, minimum_longitude))
  val minimum_latind = minimum_index.latind
  val minimum_longind = minimum_index.longind

  /**
   * Mapping of cell->locations in cell, for cell-based Naive Bayes
   * disambiguation.  The key is a tuple expressing the integer indices of the
   * latitude and longitude of the southwest corner of the cell. (Basically,
   * given an index, the latitude or longitude of the southwest corner is
   * index*degrees_per_cell, and the cell includes all locations whose
   * latitude or longitude is in the half-open interval
   * [index*degrees_per_cell, (index+1)*degrees_per_cell).
   *
   * We don't just create an array because we expect many cells to have no
   * articles in them, esp. as we decrease the cell size.  The idea is that
   * the cells provide a first approximation to the cells used to create the
   * article distributions.
   */
  var tiling_cell_to_articles = bufmap[RegularCellIndex, GeoArticle]()

  /**
   * Mapping from index of southwest corner of multi cell to corresponding
   * cell object.  A "multi cell" is made up of a square of tiling cells,
   * with the number of cells on a side determined by `width_of_multi_cell'.
   * A word distribution is associated with each multi cell.
   */
  val corner_to_multi_cell = mutable.Map[RegularCellIndex, MultiRegularCell]()

  var total_num_cells = 0

  /*************** Conversion between Cell indices and Coords *************/

  /* The different functions vary depending on where in the particular cell
     the Coord is wanted, e.g. one of the corners or the center. */

  /**
   * Convert a coordinate to the indices of the southwest corner of the
   * corresponding tiling cell.
   */
  def coord_to_tiling_cell_index(coord: Coord) = {
    val latind = floor(coord.lat / degrees_per_cell).toInt
    val longind = floor(coord.long / degrees_per_cell).toInt
    RegularCellIndex(latind, longind)
  }

  /**
   * Convert a coordinate to the indices of the southwest corner of the
   * corresponding multi cell.
   */
  def coord_to_multi_cell_index(coord: Coord) = {
    // When width_of_multi_cell = 1, don't subtract anything.
    // When width_of_multi_cell = 2, subtract 0.5*degrees_per_cell.
    // When width_of_multi_cell = 3, subtract degrees_per_cell.
    // When width_of_multi_cell = 4, subtract 1.5*degrees_per_cell.
    // In general, subtract (width_of_multi_cell-1)/2.0*degrees_per_cell.

    // Compute the indices of the southwest cell
    val subval = (width_of_multi_cell - 1) / 2.0 * degrees_per_cell
    coord_to_tiling_cell_index(
      Coord(coord.lat - subval, coord.long - subval))
  }

  /**
   * Convert a fractional cell index to the corresponding coordinate.  Useful
   * for indices not referring to the corner of a cell.
   * 
   * @seealso #cell_index_to_coord
   */
  def fractional_cell_index_to_coord(index: FractionalRegularCellIndex,
    method: String = "coerce-warn") = {
    Coord(index.latind * degrees_per_cell, index.longind * degrees_per_cell,
      method)
  }

  /**
   * Convert cell indices to the corresponding coordinate.  This can also
   * be used to find the coordinate of the southwest corner of a tiling cell
   * or multi cell, as both are identified by the cell indices of
   * their southwest corner.
   */
  def cell_index_to_coord(index: RegularCellIndex,
    method: String = "coerce-warn") =
    fractional_cell_index_to_coord(index.toFractional, method)

  /** 
   * Add 'offset' to both latind and longind of 'index' and then convert to a
   * coordinate.  Coerce the coordinate to be within bounds.
   */
  def offset_cell_index_to_coord(index: RegularCellIndex,
    offset: Double) = {
    fractional_cell_index_to_coord(
      FractionalRegularCellIndex(index.latind + offset, index.longind + offset),
      "coerce")
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * near (i.e. southwest) corner of the cell.
   */
  def tiling_cell_index_to_near_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(index)
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * center of the cell.
   */
  def tiling_cell_index_to_center_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, 0.5)
  }

  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * far (i.e. northeast) corner of the cell.
   */
  def tiling_cell_index_to_far_corner_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, 1.0)
  }
  /**
   * Convert cell indices of a tiling cell to the coordinate of the
   * near (i.e. southwest) corner of the cell.
   */
  def multi_cell_index_to_near_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(index)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * center of the cell.
   */
  def multi_cell_index_to_center_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, width_of_multi_cell / 2.0)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * far (i.e. northeast) corner of the cell.
   */
  def multi_cell_index_to_far_corner_coord(index: RegularCellIndex) = {
    offset_cell_index_to_coord(index, width_of_multi_cell)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * northwest corner of the cell.
   */
  def multi_cell_index_to_nw_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(
      RegularCellIndex(index.latind + width_of_multi_cell, index.longind),
      "coerce")
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * southeast corner of the cell.
   */
  def multi_cell_index_to_se_corner_coord(index: RegularCellIndex) = {
    cell_index_to_coord(
      RegularCellIndex(index.latind, index.longind + width_of_multi_cell),
      "coerce")
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * southwest corner of the cell.
   */
  def multi_cell_index_to_sw_corner_coord(index: RegularCellIndex) = {
    multi_cell_index_to_near_corner_coord(index)
  }

  /**
   * Convert cell indices of a multi cell to the coordinate of the
   * northeast corner of the cell.
   */
  def multi_cell_index_to_ne_corner_coord(index: RegularCellIndex) = {
    multi_cell_index_to_far_corner_coord(index)
  }

  /*************** End conversion functions *************/

  def find_best_cell_for_coord(coord: Coord) = {
    val index = coord_to_multi_cell_index(coord)
    find_cell_for_cell_index(index)
  }

  protected def find_cell_for_cell_index(index: RegularCellIndex,
    create: Boolean = false) = {
    if (!create)
      assert(all_cells_computed)
    val statcell = corner_to_multi_cell.getOrElse(index, null)
    if (statcell != null)
      statcell
    else if (!create) null
    else {
      val newstat = new MultiRegularCell(this, index)
      newstat.generate_dist()
      if (newstat.word_dist_wrapper.is_empty())
        null
      else {
        num_non_empty_cells += 1
        corner_to_multi_cell(index) = newstat
        newstat
      }
    }
  }

  def add_article_to_cell(article: GeoArticle) {
    val index = coord_to_tiling_cell_index(article.coord)
    tiling_cell_to_articles(index) += article
  }

  protected def initialize_cells() {
    val task = new MeteredTask("Earth-tiling cell", "generating non-empty")

    for (i <- minimum_latind to maximum_latind view) {
      for (j <- minimum_longind to maximum_longind view) {
        total_num_cells += 1
        val cell = find_cell_for_cell_index(RegularCellIndex(i, j),
          create = true)
        if (debug("cell") && !cell.word_dist_wrapper.is_empty)
          errprint("--> (%d,%d): %s", i, j, cell)
        task.item_processed()
      }
    }
    task.finish()

    // Save some memory by clearing this after it's not needed
    tiling_cell_to_articles = null
  }

  def iter_nonempty_cells(nonempty_word_dist: Boolean = false) = {
    assert(all_cells_computed)
    for {
      v <- corner_to_multi_cell.values
      val empty = (
        if (nonempty_word_dist) v.word_dist_wrapper.is_empty_for_word_dist()
        else v.word_dist_wrapper.is_empty())
      if (!empty)
    } yield v
  }

  /**
   * Output a "ranking grid" of information so that a nice 3-D graph
   * can be created showing the ranks of cells surrounding the true
   * cell, out to a certain distance.
   * 
   * @param pred_cells List of predicted cells, along with their scores.
   * @param true_cell True cell.
   * @param grsize Total size of the ranking grid. (For example, a total size
   *   of 21 will result in a ranking grid with the true cell and 10
   *   cells on each side shown.)
   */
  def output_ranking_grid(pred_cells: Seq[(MultiRegularCell, Double)],
    true_cell: MultiRegularCell, grsize: Int) {
    val (true_latind, true_longind) =
      (true_cell.index.latind, true_cell.index.longind)
    val min_latind = true_latind - grsize / 2
    val max_latind = min_latind + grsize - 1
    val min_longind = true_longind - grsize / 2
    val max_longind = min_longind + grsize - 1
    val grid = mutable.Map[RegularCellIndex, (GeoCell, Double, Int)]()
    for (((cell, value), rank) <- pred_cells zip (1 to pred_cells.length)) {
      val (la, lo) = (cell.index.latind, cell.index.longind)
      if (la >= min_latind && la <= max_latind &&
        lo >= min_longind && lo <= max_longind)
        grid(cell.index) = (cell, value, rank)
    }

    errprint("Grid ranking, gridsize %dx%d", grsize, grsize)
    errprint("NW corner: %s",
      multi_cell_index_to_nw_corner_coord(
        RegularCellIndex(max_latind, min_longind)))
    errprint("SE corner: %s",
      multi_cell_index_to_se_corner_coord(
        RegularCellIndex(min_latind, max_longind)))
    for (doit <- Seq(0, 1)) {
      if (doit == 0)
        errprint("Grid for ranking:")
      else
        errprint("Grid for goodness/distance:")
      for (lat <- max_latind to min_latind) {
        for (long <- fromto(min_longind, max_longind)) {
          val cellvalrank = grid.getOrElse(RegularCellIndex(lat, long), null)
          if (cellvalrank == null)
            errout(" %-8s", "empty")
          else {
            val (cell, value, rank) = cellvalrank
            val showit = if (doit == 0) rank else value
            if (lat == true_latind && long == true_longind)
              errout("!%-8.6s", showit)
            else
              errout(" %-8.6s", showit)
          }
        }
        errout("\n")
      }
    }
  }
}
