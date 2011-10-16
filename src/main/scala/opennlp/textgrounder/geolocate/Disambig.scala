////////
//////// Disambig.scala
////////
//////// Copyright (c) 2010, 2011 Ben Wing.
////////

package opennlp.textgrounder.geolocate
import KLDiv._
import NlpUtil._
import WordDist.{Word, invalid_word, memoize_word, unmemoize_word}
import OptParse._
import Distances._

import util.matching.Regex
import util.Random
import math._
import collection.mutable
import util.control.Breaks._
import java.io._

//import sys
//import os
//import os.path
//import traceback
//from itertools import *
//import random
//import gc
//import time

/////////////////////////////////////////////////////////////////////////////
//                              Documentation                              //
/////////////////////////////////////////////////////////////////////////////

////// Quick start

// This program does disambiguation of geographic names on the TR-CONLL corpus.
// It uses data from Wikipedia to do this.  It is "unsupervised" in the sense
// that it does not do any supervised learning using the correct matches
// provided in the corpus; instead, it uses them only for evaluation purposes.

/////////////////////////////////////////////////////////////////////////////
//                                  Globals                                //
/////////////////////////////////////////////////////////////////////////////

/////////////////////////////////////////////////////////////////////////////
//                               Structures                                //
/////////////////////////////////////////////////////////////////////////////

//  def print_structure(struct: Any, indent: Int=0) {
//    val indstr = " "*indent
//    if (struct == null)
//      errprint("%snull", indstr)
//    else if (struct.isInstanceOf[Tuple2[Any,Any]]) {
//      val (x,y) = struct.asInstanceOf[Tuple2[Any,Any]]
//      print_structure(List(x,y), indent)
//    } else if (!(struct.isInstanceOf[Seq[Any]]) ||
//               struct.asInstanceOf[Seq[Any]].length == 0)
//      errprint("%s%s", indstr, struct)
//    else {
//      if (struct(0).isInstanceOf[String]) {
//        errprint("%s%s:", indstr, struct.asInstanceOf[String](0))
//        indstr += "  "
//        indent += 2
//        struct = struct.slice(1)
//      }
//      for (s <- struct) {
//        if (isinstance(s, Seq))
//          print_structure(s, indent + 2)
//        else if (isinstance(s, tuple)) {
//          val (key, value) = s
//          if (isinstance(value, Seq)) {
//            errprint("%s%s:", indstr, key)
//            print_structure(value, indent + 2)
//          }
//          else
//            errprint("%s%s: %s", indstr, key, value)
//        }
//        else
//          errprint("%s%s", indstr, s)
//      }
//    }
//  }

object KMLConstants {
  // Height of highest bar in meters
  val kml_max_height = 2000000

  // Minimum and maximum colors
  val kml_mincolor = Array(255.0, 255.0, 0.0) // yellow
  val kml_maxcolor = Array(255.0, 0.0, 0.0) // red
}

/////////////////////////////////////////////////////////////////////////////
//                             Word distributions                          //
/////////////////////////////////////////////////////////////////////////////

/**
  Distribution over words corresponding to a statistical region.
 */

class RegionWordDist extends WordDist {
  /** Number of articles included in incoming-link computation. */
  var num_arts_for_links = 0
  /** Total number of incoming links. */
  var incoming_links = 0
  /** Number of articles included in word distribution. */
  var num_arts_for_word_dist = 0

  def is_empty_for_word_dist() = num_arts_for_word_dist == 0

  def is_empty() = num_arts_for_links == 0

  // Add the given articles to the total distribution seen so far
  def add_articles(articles: Iterable[StatArticle]) {
    var this_incoming_links = 0
    if (debug("lots"))
      errprint("Region dist, number of articles = %s", num_arts_for_word_dist)
    val old_total_tokens = total_tokens
    var this_num_arts_for_links = 0
    var this_num_arts_for_word_dist = 0
    for (art <- articles) {
      // Might be None, for unknown link count
      art.incoming_links match {
        case Some(x) => this_incoming_links += x
        case _ =>
      }
      this_num_arts_for_links += 1
      if (art.dist == null) {
        if (Opts.max_time_per_stage == 0 && Opts.num_training_docs == 0)
          warning("Saw article %s without distribution", art)
      } else {
        assert(art.dist.finished)
        if (art.split == "training") {
          add_word_distribution(art.dist)
          this_num_arts_for_word_dist += 1
        }
      }
    }
    num_arts_for_links += this_num_arts_for_links
    num_arts_for_word_dist = this_num_arts_for_word_dist
    incoming_links += this_incoming_links
    if (this_num_arts_for_word_dist > 0 && debug("lots")) {
      errprint("""--> Finished processing, number articles handled = %s/%s,
    skipped articles = %s, total tokens = %s/%s, incoming links = %s/%s""",
        this_num_arts_for_word_dist,
        num_arts_for_word_dist,
        this_num_arts_for_links - this_num_arts_for_word_dist,
        total_tokens - old_total_tokens,
        total_tokens, this_incoming_links, incoming_links)
    }
  }

  override def finish(minimum_word_count: Int = 0) {
    super.finish(minimum_word_count = minimum_word_count)

    if (debug("lots")) {
      errprint("""For region dist, num articles = %s, total tokens = %s,
    unseen_mass = %s, incoming links = %s, overall unseen mass = %s""",
        num_arts_for_word_dist, total_tokens,
        unseen_mass, incoming_links,
        overall_unseen_mass)
    }
  }

  // For a document described by its distribution 'worddist', return the
  // log probability log p(worddist|reg) using a Naive Bayes algorithm.
  def get_nbayes_logprob(worddist: WordDist) = {
    var logprob = 0.0
    for ((word, count) <- worddist.counts) {
      val value = lookup_word(word)
      if (value <= 0) {
        // FIXME: Need to figure out why this happens (perhaps the word was
        // never seen anywhere in the training data? But I thought we have
        // a case to handle that) and what to do instead.
        errprint("Warning! For word %s, prob %s out of range", word, value)
      } else
        logprob += log(value)
    }
    // FIXME: Also use baseline (prior probability)
    logprob
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Region distributions                        //
/////////////////////////////////////////////////////////////////////////////

// A simple distribution associating a probability with each region.

class RegionDist(
  val regionprobs: mutable.Map[StatRegion, Double]
) {
  def get_ranked_regions() = {
    // sort by second element of tuple, in reverse order
    regionprobs.toSeq sortWith (_._2 > _._2)
  }
}

// Distribution over regions, as might be attached to a word.  If we have a
// set of regions, each with a word distribution, then we can imagine
// conceptually inverting the process to generate a region distribution over
// words.  Basically, for a given word, look to see what its probability is
// in all regions; normalize, and we have a region distribution.

// Fields defined:
//
//   word: Word for which the region is computed
//   regionprobs: Hash table listing probabilities associated with regions

class WordRegionDist(
  val word: Word
) extends RegionDist(mutable.Map[StatRegion, Double]()) {
  var normalized = false

  protected def init() {
    // It's expensive to compute the value for a given word so we cache word
    // distributions.
    var totalprob = 0.0
    // Compute and store un-normalized probabilities for all regions
    for (reg <- StatRegion.iter_nonempty_regions(nonempty_word_dist = true)) {
      val prob = reg.worddist.lookup_word(word)
      // Another way of handling zero probabilities.
      /// Zero probabilities are just a bad idea.  They lead to all sorts of
      /// pathologies when trying to do things like "normalize".
      //if (prob == 0.0)
      //  prob = 1e-50
      regionprobs(reg) = prob
      totalprob += prob
    }
    // Normalize the probabilities; but if all probabilities are 0, then
    // we can't normalize, so leave as-is. (FIXME When can this happen?
    // It does happen when you use --mode=generate-kml and specify words
    // that aren't seen.  In other circumstances, the smoothing ought to
    // ensure that 0 probabilities don't exist?  Anything else I missed?)
    if (totalprob != 0) {
      normalized = true
      for ((reg, prob) <- regionprobs)
        regionprobs(reg) /= totalprob
    } else
      normalized = false
  }

  init()

  // Convert region to a KML file showing the distribution
  def generate_kml_file(filename: String) {
    import KMLConstants._
    val xform = if (Opts.kml_transform == "log") (x: Double) => log(x)
    else if (Opts.kml_transform == "logsquared") (x: Double) => -log(x) * log(x)
    else (x: Double) => x

    val minxformprob = xform(regionprobs.values min)
    val maxxformprob = xform(regionprobs.values max)

    // Generate KML for a single region
    def one_reg_kml(reg: StatRegion, prob: Double) = {
      val (latind, longind) = (reg.latind.get, reg.longind.get)
      val offprob = xform(prob) - minxformprob
      val fracprob = offprob / (maxxformprob - minxformprob)
      val swcoord = stat_region_indices_to_near_corner_coord(latind, longind)
      val necoord = stat_region_indices_to_far_corner_coord(latind, longind)
      val nwcoord = Coord(necoord.lat, swcoord.long)
      val secoord = Coord(swcoord.lat, necoord.long)
      val center = stat_region_indices_to_center_coord(latind, longind)
      var coordtext = "\n"
      for (coord <- Seq(swcoord, nwcoord, necoord, secoord, swcoord)) {
        val lat = (center.lat + coord.lat) / 2
        val long = (center.long + coord.long) / 2
        coordtext += "%s,%s,%s\n" format (long, lat, fracprob * kml_max_height)
      }
      val name =
        if (reg.most_popular_article != null) reg.most_popular_article.title
        else ""

      // Placemark indicating name
      // !!PY2SCALA: BEGIN_PASSTHRU
      // Because it tries to frob the # sign
      val name_placemark =
        <Placemark>
          <name>{ name }</name>
          ,
          <Region>
            <LatLonAltBox>
              <north>{ ((center.lat + necoord.lat) / 2).toString }</north>
              <south>{ ((center.lat + swcoord.lat) / 2).toString }</south>
              <east>{ ((center.long + necoord.long) / 2).toString }</east>
              <west>{ ((center.long + swcoord.long) / 2).toString }</west>
            </LatLonAltBox>
            <Lod>
              <minLodPixels>16</minLodPixels>
            </Lod>
          </Region>
          <styleURL>#bar</styleURL>
          <Point>
            <coordinates>{ "%s,%s" format (center.long, center.lat) }</coordinates>
          </Point>
        </Placemark>
      // !!PY2SCALA: END_PASSTHRU

      // Interpolate colors
      val color = Array(0.0, 0.0, 0.0)
      for (i <- 1 to 3) {
        color(i) = (kml_mincolor(i) +
          fracprob * (kml_maxcolor(i) - kml_mincolor(i)))
      }
      // Original color dc0155ff
      //rgbcolor = "dc0155ff"
      val revcol = color.reverse
      val rgbcolor = "ff%02x%02x%02x" format (revcol(0), revcol(1), revcol(2))

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

    def yield_reg_kml() {
      for {
        (reg, prob) <- regionprobs
        kml <- one_reg_kml(reg, prob)
        expr <- kml
      } yield expr
    }

    val allregkml = yield_reg_kml()

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
            <description>{ "Region distribution for word '%s'" format unmemoize_word(word) }</description>
            <LookAt>
              <latitude>42</latitude>
              <longitude>-102</longitude>
              <altitude>0</altitude>
              <range>5000000</range>
              <tilt>53.454348562403</tilt>
              <heading>0</heading>
            </LookAt>
            { allregkml }
          </Folder>
        </Document>
      </kml>

    xml.XML.save(filename, kml)
  }
}

object RegionDist {
  var cached_dists: LRUCache[Word, WordRegionDist] = null

  // Return a region distribution over a given word, using a least-recently-used
  // cache to optimize access.
  def get_region_dist(word: Word) = {
    if (cached_dists == null)
      cached_dists = new LRUCache(maxsize = Opts.lru_cache_size)
    cached_dists.get(word) match {
      case Some(dist) => dist
      case None => {
        val dist = new WordRegionDist(word)
        cached_dists(word) = dist
        dist
      }
    }
  }

  // Return a region distribution over a distribution over words.  This works
  // by adding up the distributions of the individual words, weighting by
  // the count of the each word.
  def get_region_dist_for_word_dist(worddist: WordDist) = {
    val regprobs = doublemap[StatRegion]()
    for ((word, count) <- worddist.counts) {
      val dist = get_region_dist(word)
      for ((reg, prob) <- dist.regionprobs)
        regprobs(reg) += count * prob
    }
    val totalprob = (regprobs.values sum)
    for ((reg, prob) <- regprobs)
      regprobs(reg) /= totalprob
    new RegionDist(regprobs)
  }
}

/////////////////////////////////////////////////////////////////////////////
//                           Geographic locations                          //
/////////////////////////////////////////////////////////////////////////////

///////////// statistical regions ////////////

// This class contains values used in computing the distribution over all
// locations in the statistical region surrounding the locality in question.
// The statistical region is currently defined as a square of NxN tiling
// regions, for N = width_of_stat_region.
// The following fields are defined: 
//
//   latind, longind: Region indices of southwest-most tiling region in
//                    statistical region.
//   worddist: Distribution corresponding to region.

class StatRegion(
  val latind: Option[Regind],
  val longind: Option[Regind]) {
  val worddist = new RegionWordDist()
  var most_popular_article: StatArticle = null
  var mostpopart_links = 0

  def boundstr() = {
    if (!latind.isEmpty) {
      val near =
        stat_region_indices_to_near_corner_coord(latind.get, longind.get)
      val far =
        stat_region_indices_to_far_corner_coord(latind.get, longind.get)
      "%s-%s" format (near, far)
    } else "nowhere"
  }

  override def toString() = {
    val unfinished = if (worddist.finished) "" else ", unfinished"
    val contains =
      if (most_popular_article != null)
        ", most-pop-art %s(%d links)" format (
          most_popular_article, mostpopart_links)
      else ""

    "StatRegion(%s%s%s, %d articles(dist), %d articles(links), %d links)" format (
      boundstr(), unfinished, contains,
      worddist.num_arts_for_word_dist, worddist.num_arts_for_links,
      worddist.incoming_links)
  }

  // def __repr__() = {
  //   toString.encode("utf-8")
  // }

  def shortstr() = {
    var str = "Region %s" format boundstr()
    val mostpop = most_popular_article
    if (mostpop != null)
      str += ", most-popular %s" format mostpop.shortstr()
    str
  }

  def struct() =
    <StatRegion>
      <bounds>{ boundstr() }</bounds>
      <finished>{ worddist.finished }</finished>
      {
        if (most_popular_article != null)
          (<mostPopularArticle>most_popular_article.struct()</mostPopularArticle>
           <mostPopularArticleLinks>mostpopart_links</mostPopularArticleLinks>)
      }
      <numArticlesDist>{ worddist.num_arts_for_word_dist }</numArticlesDist>
      <numArticlesLink>{ worddist.num_arts_for_links }</numArticlesLink>
      <incomingLinks>{ worddist.incoming_links }</incomingLinks>
    </StatRegion>

  // Generate the distribution for a statistical region from the tiling regions.
  def generate_dist() {

    val reglat = latind.get
    val reglong = longind.get

    if (debug("lots")) {
      errprint("Generating distribution for statistical region centered at %s",
        region_indices_to_coord(reglat, reglong))
    }

    // Accumulate counts for the given region
    def process_one_region(latind: Regind, longind: Regind) {
      val arts =
        StatRegion.tiling_region_to_articles.getOrElse((latind, longind), null)
      if (arts == null)
        return
      if (debug("lots")) {
        errprint("--> Processing tiling region %s",
          region_indices_to_coord(latind, longind))
      }
      worddist.add_articles(arts)
      for (art <- arts) {
        if (art.incoming_links != None &&
            art.incoming_links.get > mostpopart_links) {
          mostpopart_links = art.incoming_links.get
          most_popular_article = art
        }
      }
    }

    // Process the tiling regions making up the statistical region;
    // but be careful around the edges.  Truncate the latitude, wrap the
    // longitude.
    for (
      i <- reglat until (maximum_latind + 1 min
        reglat + width_of_stat_region)
    ) {
      for (j <- reglong until reglong + width_of_stat_region) {
        var jj = j
        if (jj > maximum_longind) jj -= 360
        process_one_region(i, jj)
      }
    }

    worddist.finish(minimum_word_count = Opts.minimum_word_count)
  }
}

object StatRegion {
  // Mapping of region->locations in region, for region-based Naive Bayes
  // disambiguation.  The key is a tuple expressing the integer indices of the
  // latitude and longitude of the southwest corner of the region. (Basically,
  // given an index, the latitude or longitude of the southwest corner is
  // index*degrees_per_region, and the region includes all locations whose
  // latitude or longitude is in the half-open interval
  // [index*degrees_per_region, (index+1)*degrees_per_region).
  //
  // We don't just create an array because we expect many regions to have no
  // articles in them, esp. as we decrease the region size.  The idea is that
  // the regions provide a first approximation to the regions used to create the
  // article distributions.
  var tiling_region_to_articles = bufmap[(Regind, Regind), StatArticle]()

  // Mapping from center of statistical region to corresponding region object.
  // A "statistical region" is made up of a square of tiling regions, with
  // the number of regions on a side determined by `width_of_stat_region'.  A
  // word distribution is associated with each statistical region.
  val corner_to_stat_region = mutable.Map[(Regind, Regind), StatRegion]()

  var empty_stat_region: StatRegion = null // Can't compute this until class is initialized
  var all_regions_computed = false
  var num_empty_regions = 0
  var num_non_empty_regions = 0
  var total_num_arts_for_word_dist = 0
  var total_num_arts_for_links = 0

  // Find the correct StatRegion for the given coordinates.
  // If none, create the region.
  def find_region_for_coord(coord: Coord) = {
    val (latind, longind) = coord_to_stat_region_indices(coord)
    find_region_for_region_indices(latind, longind)
  }

  // Find the StatRegion with the given indices at the southwest point.
  // If none, create the region unless 'no_create' is true.  Otherwise, if
  // 'no_create_empty' is true and the region is empty, a default empty
  // region is returned.
  def find_region_for_region_indices(latind: Regind, longind: Regind,
    no_create: Boolean = false, no_create_empty: Boolean = false): StatRegion = {
    var statreg = corner_to_stat_region.getOrElse((latind, longind), null)
    if (statreg == null) {
      if (no_create)
        return null
      if (all_regions_computed) {
        if (empty_stat_region == null) {
          empty_stat_region = new StatRegion(None, None)
          empty_stat_region.worddist.finish()
        }
        return empty_stat_region
      }
      statreg = new StatRegion(Some(latind), Some(longind))
      statreg.generate_dist()
      val empty = statreg.worddist.is_empty()
      if (empty)
        num_empty_regions += 1
      else
        num_non_empty_regions += 1
      if (!empty || !no_create_empty)
        corner_to_stat_region((latind, longind)) = statreg
    }
    return statreg
  }

  // Generate all StatRegions that are non-empty.  Don't do anything if
  // called multiple times.
  def initialize_regions() {
    if (all_regions_computed)
      return

    errprint("Generating all non-empty statistical regions...")
    val status = new StatusMessage("statistical region")

    for (i <- minimum_latind to maximum_latind view) {
      for (j <- minimum_longind to maximum_longind view) {
        val reg = find_region_for_region_indices(i, j, no_create_empty = true)
        if (debug("region") && !reg.worddist.is_empty)
          errprint("--> (%d,%d): %s", i, j, reg)
        status.item_processed()
      }
    }
    all_regions_computed = true

    total_num_arts_for_links = 0
    total_num_arts_for_word_dist = 0
    for (reg <- StatRegion.iter_nonempty_regions()) {
      total_num_arts_for_word_dist += reg.worddist.num_arts_for_word_dist
      total_num_arts_for_links += reg.worddist.num_arts_for_links
    }

    errprint("Number of non-empty regions: %s", num_non_empty_regions)
    errprint("Number of empty regions: %s", num_empty_regions)
    // Save some memory by clearing this after it's not needed
    tiling_region_to_articles = null
    StatArticleTable.table.clear_training_article_distributions()
  }

  // Add the given article to the region map, which covers the earth in regions
  // of a particular size to aid in computing the regions used in region-based
  // Naive Bayes.
  def add_article_to_region(article: StatArticle) {
    val (latind, longind) = coord_to_tiling_region_indices(article.coord)
    tiling_region_to_articles((latind, longind)) += article
  }

  // Iterate over all non-empty regions.  If 'nonempty_word_dist' is given,
  // distributions must also have a non-empty word distribution; otherwise,
  // they just need to have at least one point in them. (Not all points
  // have word distributions, esp. when --max-time-per-stage is set so
  // that we only load the word distributions for a fraction of the whole
  // set of articles with distributions.)
  def iter_nonempty_regions(nonempty_word_dist: Boolean = false) = {
    assert(all_regions_computed)
    for {
      v <- corner_to_stat_region.values
      val empty = (
        if (nonempty_word_dist) v.worddist.is_empty_for_word_dist()
        else v.worddist.is_empty())
      if (!empty)
    } yield v
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Wikipedia articles                          //
/////////////////////////////////////////////////////////////////////////////

//////////////////////  Article table

// Class maintaining tables listing all articles and mapping between
// names, ID's and articles.  Objects corresponding to redirect articles
// should not be present anywhere in this table; instead, the name of the
// redirect article should point to the article object for the article
// pointed to by the redirect.
class StatArticleTable {
  // Mapping from article names to StatArticle objects, using the actual case of
  // the article.
  val name_to_article = mutable.Map[String, StatArticle]()

  // List of articles in each split.
  val articles_by_split = bufmap[String,StatArticle]()

  // Num of articles with word-count information but not in table.
  var num_articles_with_word_counts_but_not_in_table = 0

  // Num of articles with word-count information (whether or not in table).
  var num_articles_with_word_counts = 0

  // Num of articles in each split with word-count information seen.
  val num_word_count_articles_by_split = intmap[String]()

  // Num of articles in each split with a computed distribution.
  // (Not the same as the previous since we don't compute the distribution of articles in
  // either the test or dev set depending on which one is used.)
  val num_dist_articles_by_split = intmap[String]()

  // Total # of word tokens for all articles in each split.
  val word_tokens_by_split = intmap[String]()

  // Total # of incoming links for all articles in each split.
  val incoming_links_by_split = intmap[String]()

  // Map from short name (lowercased) to list of Wikipedia articles.  The
  // short name for an article is computed from the article's name.  If
  // the article name has a comma, the short name is the part before the
  // comma, e.g. the short name of "Springfield, Ohio" is "Springfield".
  // If the name has no comma, the short name is the same as the article
  // name.  The idea is that the short name should be the same as one of
  // the toponyms used to refer to the article.
  val short_lower_name_to_articles = bufmap[String,StatArticle]()

  // Map from tuple (NAME, DIV) for Wikipedia articles of the form
  // "Springfield, Ohio", lowercased.
  val lower_name_div_to_articles = bufmap[(String, String), StatArticle]()

  // For each toponym, list of Wikipedia articles matching the name.
  val lower_toponym_to_article = bufmap[String,StatArticle]()

  // Mapping from lowercased article names to TopoArticle objects
  val lower_name_to_articles = bufmap[String,StatArticle]()

  // Look up an article named NAME and return the associated article.
  // Note that article names are case-sensitive but the first letter needs to
  // be capitalized.
  def lookup_article(name: String) = {
    assert(name != null)
    name_to_article.getOrElse(capfirst(name), null)
  }

  // Record the article as having NAME as one of its names (there may be
  // multiple names, due to redirects).  Also add to related lists mapping
  // lowercased form, short form, etc.
  def record_article_name(name: String, art: StatArticle) {
    // Must pass in properly cased name
    // errprint("name=%s, capfirst=%s", name, capfirst(name))
    // println("length=%s" format name.length)
    // if (name.length > 1) {
    //   println("name(0)=0x%x" format name(0).toInt)
    //   println("name(1)=0x%x" format name(1).toInt)
    //   println("capfirst(0)=0x%x" format capfirst(name)(0).toInt)
    // }
    assert(name == capfirst(name))
    name_to_article(name) = art
    val loname = name.toLowerCase
    lower_name_to_articles(loname) += art
    val (short, div) = Article.compute_short_form(loname)
    if (div != null)
      lower_name_div_to_articles((short, div)) += art
    short_lower_name_to_articles(short) += art
    if (!(lower_toponym_to_article(loname) contains art))
      lower_toponym_to_article(loname) += art
    if (short != loname && !(lower_toponym_to_article(short) contains art))
      lower_toponym_to_article(short) += art
  }
  
  // Record either a normal article ('artfrom' same as 'artto') or a
  // redirect ('artfrom' redirects to 'artto').
  def record_article(artfrom: StatArticle, artto: StatArticle) {
    record_article_name(artfrom.title, artto)
    val redir = !(artfrom eq artto)
    val split = artto.split
    val fromlinks = artfrom.adjusted_incoming_links
    incoming_links_by_split(split) += fromlinks
    if (!redir) {
      articles_by_split(split) += artto
    } else if (fromlinks != 0) {
      // Add count of links pointing to a redirect to count of links
      // pointing to the article redirected to, so that the total incoming
      // link count of an article includes any redirects to that article.
      artto.incoming_links = Some(artto.adjusted_incoming_links + fromlinks)
    }
  }

  def create_article(params: Map[String, String]) = new StatArticle(params)

  def read_article_data(filename: String) {
    val redirects = mutable.Buffer[StatArticle]()

    def process(params: Map[String, String]) {
      val art = create_article(params)
      if (art.namespace != "Main")
        return
      if (art.redir.length > 0)
        redirects += art
      else if (art.coord != null) {
        record_article(art, art)
        StatRegion.add_article_to_region(art)
      }
    }

    ArticleData.read_article_data_file(filename, process,
      maxtime = Opts.max_time_per_stage)

    for (x <- redirects) {
      val redart = lookup_article(x.redir)
      if (redart != null)
        record_article(x, redart)
    }
  }

  def finish_article_distributions() {
    // Figure out the value of OVERALL_UNSEEN_MASS for each article.
    for ((split, table) <- articles_by_split) {
      var totaltoks = 0
      var numarts = 0
      for (art <- table) {
        if (art.dist != null) {
          art.dist.finish(minimum_word_count = Opts.minimum_word_count)
          totaltoks += art.dist.total_tokens
          numarts += 1
        }
      }
      num_dist_articles_by_split(split) = numarts
      word_tokens_by_split(split) = totaltoks
    }
  }

  def clear_training_article_distributions() {
    for (art <- articles_by_split("training"))
      art.dist = null
  }

  // Parse the result of a previous run of --output-counts and generate
  // a unigram distribution for Naive Bayes matching.  We do a simple version
  // of Good-Turing smoothing where we assign probability mass to unseen
  // words equal to the probability mass of all words seen once, and rescale
  // the remaining probabilities accordingly.

  def read_word_counts(filename: String) {
    val initial_dynarr_size = 1000
    val keys_dynarr =
      new DynamicArray[WordDist.Word](initial_alloc = initial_dynarr_size)
    val values_dynarr =
      new DynamicArray[Int](initial_alloc = initial_dynarr_size)

    // This is basically a one-off debug statement because of the fact that
    // the experiments published in the paper used a word-count file generated
    // using an older algorithm for determining the geotagged coordinate of
    // a Wikipedia article.  We didn't record the corresponding article-data
    // file, so we need a way of regenerating it using the intersection of
    // articles in the article-data file we actually used for the experiments
    // and the word-count file we used.
    var stream: PrintStream = null
    var writer: ArticleWriter = null
    if (debug("wordcountarts")) {
      // Change this if you want a different file name
      val wordcountarts_filename = "wordcountarts-combined-article-data.txt"
      stream = openw(wordcountarts_filename)
      // See write_article_data_file() in ArticleData.scala
      writer =
        new ArticleWriter(stream, ArticleData.combined_article_data_outfields)
      writer.output_header()
    }

    var total_tokens = 0
    var title = null: String

    def one_article_probs() {
      if (total_tokens == 0) return
      val art = lookup_article(title)
      if (art == null) {
        warning("Skipping article %s, not in table", title)
        num_articles_with_word_counts_but_not_in_table += 1
        return
      }
      if (debug("wordcountarts"))
        writer.output_row(art)
      num_word_count_articles_by_split(art.split) += 1
      // If we are evaluating on the dev set, skip the test set and vice
      // versa, to save memory and avoid contaminating the results.
      if (art.split != "training" && art.split != Opts.eval_set)
        return
      // Don't train on test set
      art.dist = new WordDist(keys_dynarr.array, values_dynarr.array,
        note_globally = (art.split == "training"))
    }

    errprint("Reading word counts from %s...", filename)
    val status = new StatusMessage("article")

    // Written this way because there's another line after the for loop,
    // corresponding to the else clause of the Python for loop
    breakable {
      for (line <- openr(filename)) {
        if (line.startsWith("Article title: ")) {
          if (title != null)
            one_article_probs()
          // Stop if we've reached the maximum
          if (status.item_processed(maxtime = Opts.max_time_per_stage))
            break
          if ((Opts.num_training_docs > 0 &&
            status.num_processed() >= Opts.num_training_docs)) {
            errprint("")
            errprint("Finishing reading word counts after %d documents",
              status.num_processed())
            break
          }

          // Extract title and set it
          val titlere = "Article title: (.*)$".r
          line match {
            case titlere(ti) => title = ti
            case _ => assert(false)
          }
          keys_dynarr.clear()
          values_dynarr.clear()
          total_tokens = 0
        } else if (line.startsWith("Article coordinates) ") ||
          line.startsWith("Article ID: "))
          ()
        else {
          val linere = "(.*) = ([0-9]+)$".r
          line match {
            case linere(xword, xcount) => {
              var word = xword
              if (!Opts.preserve_case_words) word = word.toLowerCase
              val count = xcount.toInt
              if (!(Stopwords.stopwords contains word) ||
                Opts.include_stopwords_in_article_dists) {
                total_tokens += count
                keys_dynarr += memoize_word(word)
                values_dynarr += count
              }
            }
            case _ =>
              warning("Strange line, can't parse: title=%s: line=%s",
                title, line)
          }
        }
      }
      one_article_probs()
    }

    if (debug("wordcountarts"))
      stream.close()
    errprint("Finished reading distributions from %s articles.", status.num_processed())
    num_articles_with_word_counts = status.num_processed()
    output_resource_usage()
  }

  def finish_word_counts() {
    WordDist.finish_global_distribution()
    finish_article_distributions()
    errprint("")
    errprint("-------------------------------------------------------------------------")
    errprint("Article count statistics:")
    var total_arts_in_table = 0
    var total_arts_with_word_counts = 0
    var total_arts_with_dists = 0
    for ((split, totaltoks) <- word_tokens_by_split) {
      errprint("For split '%s':", split)
      val arts_in_table = articles_by_split(split).length
      val arts_with_word_counts = num_word_count_articles_by_split(split)
      val arts_with_dists = num_dist_articles_by_split(split)
      total_arts_in_table += arts_in_table
      total_arts_with_word_counts += arts_with_word_counts
      total_arts_with_dists += arts_with_dists
      errprint("  %s articles in article table", arts_in_table)
      errprint("  %s articles with word counts seen (and in table)", arts_with_word_counts)
      errprint("  %s articles with distribution computed, %s total tokens, %.2f tokens/article",
        arts_with_dists, totaltoks,
        // Avoid division by zero
        totaltoks.toDouble / (arts_in_table + 1e-100))
    }
    errprint("Total: %s articles with word counts seen",
      num_articles_with_word_counts)
    errprint("Total: %s articles in article table", total_arts_in_table)
    errprint("Total: %s articles with word counts seen but not in article table",
      num_articles_with_word_counts_but_not_in_table)
    errprint("Total: %s articles with word counts seen (and in table)",
      total_arts_with_word_counts)
    errprint("Total: %s articles with distribution computed",
      total_arts_with_dists)
  }

  def construct_candidates(toponym: String) = {
    val lotop = toponym.toLowerCase
    lower_toponym_to_article(lotop)
  }

  def word_is_toponym(word: String) = {
    val lw = word.toLowerCase
    lower_toponym_to_article contains lw
  }
}

object StatArticleTable {
  // Currently only one StatArticleTable object
  var table: StatArticleTable = null
}

///////////////////////// Articles

// A Wikipedia article for geotagging.

class StatArticle(params: Map[String, String]) extends Article(params) {
  // Object containing word distribution of this article.
  var dist: WordDist = null

  override def toString() = {
    var coordstr = if (coord != null) " at %s" format coord else ""
    val redirstr =
      if (redir.length > 0) ", redirect to %s" format redir else ""
    "%s(%s)%s%s" format (title, id, coordstr, redirstr)
  }

  // def __repr__() = "Article(%s)" format toString.encode("utf-8")

  def shortstr() = "%s" format title

  def struct() =
    <StatArticle>
      <title>{ title }</title>
      <id>{ id }</id>
      {
        if (coord != null)
          <location>{ coord }</location>
      }
      {
        if (redir.length > 0)
          <redirectTo>{ redir }</redirectTo>
      }
    </StatArticle>

  def distance_to_coord(coord2: Coord) = spheredist(coord, coord2)
}

/////////////////////////////////////////////////////////////////////////////
//                             Accumulate results                          //
/////////////////////////////////////////////////////////////////////////////

// incorrect_reasons is a map from ID's for reasons to strings describing
// them.
class Eval(incorrect_reasons: Map[String, String]) {
  // Statistics on the types of instances processed
  // Total number of instances
  var total_instances = 0
  var correct_instances = 0
  var incorrect_instances = 0
  val other_stats = intmap[String]()
  // Map from reason ID's to counts
  var results = intmap[String]()

  def record_result(correct: Boolean, reason: String = null) {
    if (reason != null)
      assert(incorrect_reasons.keySet contains reason)
    total_instances += 1
    if (correct)
      correct_instances += 1
    else {
      incorrect_instances += 1
      if (reason != null)
        results(reason) += 1
    }
  }

  def record_other_stat(othertype: String) {
    other_stats(othertype) += 1
  }

  def output_fraction(header: String, amount: Int, total: Int) {
    if (amount > total) {
      warning("Something wrong: Fractional quantity %s greater than total %s",
        amount, total)
    }
    var percent =
      if (total == 0) "indeterminate percent"
      else "%5.2f%%" format (100 * amount.toDouble / total)
    errprint("%s = %s/%s = %s", header, amount, total, percent)
  }

  def output_correct_results() {
    output_fraction("Percent correct", correct_instances,
      total_instances)
  }

  def output_incorrect_results() {
    output_fraction("Percent incorrect", incorrect_instances,
      total_instances)
    for ((reason, descr) <- incorrect_reasons) {
      output_fraction("  %s" format descr, results(reason), total_instances)
    }
  }

  def output_other_stats() {
    for ((ty, count) <- other_stats)
      errprint("%s = %s", ty, count)
  }

  def output_results() {
    if (total_instances == 0) {
      warning("Strange, no instances found at all; perhaps --eval-format is incorrect?")
      return
    }
    errprint("Number of instances = %s", total_instances)
    output_correct_results()
    output_incorrect_results()
    output_other_stats()
  }
}

class EvalWithRank(
  max_rank_for_credit: Int = 10
) extends Eval(Map[String, String]()) {
  val incorrect_by_exact_rank = intmap[Int]()
  val correct_by_up_to_rank = intmap[Int]()
  var incorrect_past_max_rank = 0
  var total_credit = 0

  def record_result(rank: Int) {
    assert(rank >= 1)
    val correct = rank == 1
    super.record_result(correct, reason = null)
    if (rank <= max_rank_for_credit) {
      total_credit += max_rank_for_credit + 1 - rank
      incorrect_by_exact_rank(rank) += 1
      for (i <- rank to max_rank_for_credit)
        correct_by_up_to_rank(i) += 1
    } else
      incorrect_past_max_rank += 1
  }

  override def output_correct_results() {
    super.output_correct_results()
    val possible_credit = max_rank_for_credit * total_instances
    output_fraction("Percent correct with partial credit",
      total_credit, possible_credit)
    for (i <- 2 to max_rank_for_credit) {
      output_fraction("  Correct is at or above rank %s" format i,
        correct_by_up_to_rank(i), total_instances)
    }
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
    for (i <- 2 to max_rank_for_credit) {
      output_fraction("  Incorrect, with correct at rank %s" format i,
        incorrect_by_exact_rank(i),
        total_instances)
    }
    output_fraction("  Incorrect, with correct not in top %s" format
      max_rank_for_credit,
      incorrect_past_max_rank, total_instances)
  }
}

class GeotagDocumentEval(
  max_rank_for_credit: Int = 10) extends EvalWithRank(max_rank_for_credit) {
  val true_dists = mutable.Buffer[Double]()
  val degree_dists = mutable.Buffer[Double]()

  def record_result(rank: Int, true_dist: Double, degree_dist: Double) {
    super.record_result(rank)
    true_dists += true_dist
    degree_dists += degree_dist
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
    def miles_and_km(miledist: Double) = {
      val km_per_mile = 1.609
      "%.2f miles (%.2f km)" format (miledist, miledist * km_per_mile)
    }
    errprint("  Mean true error distance = %s",
      miles_and_km(mean(true_dists)))
    errprint("  Median true error distance = %s",
      miles_and_km(median(true_dists)))
    errprint("  Mean degree error distance = %.2f degrees",
      mean(degree_dists))
    errprint("  Median degree error distance = %.2f degrees",
      median(degree_dists))
  }
}

//////// Results for geotagging documents/articles

class GeotagDocumentResults {

  def create_doc() = new GeotagDocumentEval()
  val all_document = create_doc()

  // naitr = "num articles in true region"
  val docs_by_naitr = new IntTableByRange(Seq(1, 10, 25, 100), create_doc _)

  // Results for documents where the location is at a certain distance
  // from the center of the true statistical region.  The key is measured in
  // fractions of a tiling region (determined by 'dist_fraction_increment',
  // e.g. if dist_fraction_increment = 0.25 then values in the range of
  // [0.25, 0.5) go in one bin, [0.5, 0.75) go in another, etc.).  We measure
  // distance is two ways: true distance (in miles or whatever) and "degree
  // distance", as if degrees were a constant length both latitudinally
  // and longitudinally.
  val dist_fraction_increment = 0.25
  def docmap() = defaultmap[Double, GeotagDocumentEval](create_doc())
  val docs_by_degree_dist_to_true_center = docmap()
  val docs_by_true_dist_to_true_center = docmap()

  // Similar, but distance between location and center of top predicted
  // region.
  val dist_fractions_for_error_dist = Seq(
    0.25, 0.5, 0.75, 1, 1.5, 2, 3, 4, 6, 8,
    12, 16, 24, 32, 48, 64, 96, 128, 192, 256,
    // We're never going to see these
    384, 512, 768, 1024, 1536, 2048)
  val docs_by_degree_dist_to_pred_center =
    new DoubleTableByRange(dist_fractions_for_error_dist, create_doc _)
  val docs_by_true_dist_to_pred_center =
    new DoubleTableByRange(dist_fractions_for_error_dist, create_doc _)

  def record_geotag_document_result(rank: Int, coord: Coord,
    pred_latind: Regind, pred_longind: Regind,
    num_arts_in_true_region: Int,
    return_stats: Boolean = false) = {

    def degree_dist(c1: Coord, c2: Coord) = {
      sqrt((c1.lat - c2.lat) * (c1.lat - c2.lat) +
        (c1.long - c2.long) * (c1.long - c2.long))
    }

    val pred_center = stat_region_indices_to_center_coord(pred_latind, pred_longind)
    val pred_truedist = spheredist(coord, pred_center)
    val pred_degdist = degree_dist(coord, pred_center)

    all_document.record_result(rank, pred_truedist, pred_degdist)
    val naitr = docs_by_naitr.get_collector(num_arts_in_true_region)
    naitr.record_result(rank, pred_truedist, pred_degdist)

    val (true_latind, true_longind) = coord_to_stat_region_indices(coord)
    val true_center = stat_region_indices_to_center_coord(true_latind, true_longind)
    val true_truedist = spheredist(coord, true_center)
    val true_degdist = degree_dist(coord, true_center)
    val fracinc = dist_fraction_increment
    val rounded_true_truedist = fracinc * floor(true_truedist / fracinc)
    val rounded_true_degdist = fracinc * floor(true_degdist / fracinc)

    docs_by_true_dist_to_true_center(rounded_true_truedist).
      record_result(rank, pred_truedist, pred_degdist)
    docs_by_degree_dist_to_true_center(rounded_true_degdist).
      record_result(rank, pred_truedist, pred_degdist)

    docs_by_true_dist_to_pred_center.get_collector(pred_truedist).
      record_result(rank, pred_truedist, pred_degdist)
    docs_by_degree_dist_to_pred_center.get_collector(pred_degdist).
      record_result(rank, pred_truedist, pred_degdist)

    if (return_stats) {
      Map("pred_center" -> pred_center,
        "pred_truedist" -> pred_truedist,
        "pred_degdist" -> pred_degdist,
        "true_center" -> true_center,
        "true_truedist" -> true_truedist,
        "true_degdist" -> true_degdist)
    } else Map[String, Double]()
  }

  def record_geotag_document_other_stat(othertype: String) {
    all_document.record_other_stat(othertype)
  }

  def output_geotag_document_results(all_results: Boolean = false) {
    errprint("")
    errprint("Results for all documents/articles:")
    all_document.output_results()
    //if (all_results)
    if (false) {
      errprint("")
      for ((lower, upper, obj) <- docs_by_naitr.iter_ranges()) {
        errprint("")
        errprint("Results for documents/articles where number of articles")
        errprint("  in true region is in the range [%s,%s]:",
          lower, upper - 1)
        obj.output_results()
      }
      errprint("")
      for (
        (truedist, obj) <- docs_by_true_dist_to_true_center.toSeq sortBy (_._1)
      ) {
        val lowrange = truedist * Opts.miles_per_region
        val highrange = ((truedist + dist_fraction_increment) *
          Opts.miles_per_region)
        errprint("")
        errprint("Results for documents/articles where distance to center")
        errprint("  of true region in miles is in the range [%.2f,%.2f):",
          lowrange, highrange)
        obj.output_results()
      }
      errprint("")
      for (
        (degdist, obj) <- docs_by_degree_dist_to_true_center.toSeq sortBy (_._1)
      ) {
        val lowrange = degdist * degrees_per_region
        val highrange = ((degdist + dist_fraction_increment) *
          degrees_per_region)
        errprint("")
        errprint("Results for documents/articles where distance to center")
        errprint("  of true region in degrees is in the range [%.2f,%.2f):",
          lowrange, highrange)
        obj.output_results()
      }
    }
    // FIXME: Output median and mean of true and degree error dists; also
    // maybe move this info info EvalByRank so that we can output the values
    // for each category
    errprint("")
    output_resource_usage()
  }
}

/////////////////////////////////////////////////////////////////////////////
//                             Main geotagging code                        //
/////////////////////////////////////////////////////////////////////////////

// Abstract class for reading documents from a test file and evaluating on
// them.
abstract class TestFileEvaluator(stratname: String) {
  var documents_processed = 0

  type Document

  // Return an Iterable listing the documents retrievable from the given
  // filename.
  def iter_documents(filename: String): Iterable[Document]

  // Return true if document would be skipped; false if processed and
  // evaluated.
  def would_skip_document(doc: Document, doctag: String) = false

  // Return true if document was actually processed and evaluated; false
  // if skipped.
  def evaluate_document(doc: Document, doctag: String): Boolean

  // Output results so far.  If 'isfinal', this is the last call, so
  // output more results.
  def output_results(isfinal: Boolean = false): Unit

  def evaluate_and_output_results(files: Iterable[String]) {
    val status = new StatusMessage("document")
    var last_elapsed = 0.0
    var last_processed = 0
    var skip_initial = Opts.skip_initial_test_docs
    var skip_n = 0

    def output_final_results() {
      errprint("")
      errprint("Final results for strategy %s: All %d documents processed:",
        stratname, status.num_processed())
      errprint("Ending operation at %s", curtimehuman())
      output_results(isfinal = true)
      errprint("Ending final results for strategy %s", stratname)
    }

    for (filename <- files) {
      errprint("Processing evaluation file %s...", filename)
      for (doc <- iter_documents(filename)) {
        // errprint("Processing document: %s", doc)
        val num_processed = status.num_processed()
        val doctag = "#%d" format (1 + num_processed)
        if (would_skip_document(doc, doctag))
          errprint("Skipped document %s", doc)
        else {
          var do_skip = false
          if (skip_initial != 0) {
            skip_initial -= 1
            do_skip = true
          } else if (skip_n != 0) {
            skip_n -= 1
            do_skip = true
          } else
            skip_n = Opts.every_nth_test_doc - 1
          if (do_skip)
            errprint("Passed over document %s", doctag)
          else {
            // Don't put side-effecting code inside of an assert!
            val not_skipped = evaluate_document(doc, doctag)
            assert(not_skipped)
          }
          status.item_processed()
          val new_elapsed = status.elapsed_time()
          val new_processed = status.num_processed()

          // If max # of docs reached, stop
          if ((Opts.num_test_docs > 0 &&
            new_processed >= Opts.num_test_docs)) {
            errprint("")
            errprint("Finishing evaluation after %d documents",
              new_processed)
            output_final_results()
            return
          }

          // If five minutes and ten documents have gone by, print out results
          if ((new_elapsed - last_elapsed >= 300 &&
            new_processed - last_processed >= 10)) {
            errprint("Results after %d documents (strategy %s):",
              status.num_processed(), stratname)
            output_results(isfinal = false)
            errprint("End of results after %d documents (strategy %s):",
              status.num_processed(), stratname)
            last_elapsed = new_elapsed
            last_processed = new_processed
          }
        }
      }
    }

    output_final_results()
  }
}

abstract class GeotagDocumentStrategy {
  def return_ranked_regions(worddist: WordDist): Iterable[(StatRegion, Double)]
}

class BaselineGeotagDocumentStrategy(
  baseline_strategy: String)
  extends GeotagDocumentStrategy {
  var cached_ranked_mps: Iterable[(StatRegion, Double)] = null

  def ranked_regions_random(worddist: WordDist) = {
    val regions = StatRegion.iter_nonempty_regions()
    val shuffled = (new Random()).shuffle(regions)
    (for (reg <- shuffled) yield (reg, 0.0))
  }

  def ranked_most_popular_regions(worddist: WordDist) = {
    if (cached_ranked_mps == null) {
      cached_ranked_mps = (
        (for (reg <- StatRegion.iter_nonempty_regions())
          yield (reg, (if (baseline_strategy == "internal_link")
          reg.worddist.incoming_links
        else reg.worddist.num_arts_for_links).toDouble)).
        toArray sortWith (_._2 > _._2))
    }
    cached_ranked_mps
  }

  def ranked_regions_regdist_most_common_toponym(worddist: WordDist) = {
    // Look for a toponym, then a proper noun, then any word.
    // FIXME: How can 'word' be null?
    // FIXME: Use invalid_word
    // FIXME: Should predicate be passed an index and have to do its own
    // unmemoizing?
    var maxword = worddist.find_most_common_word(
      word => word(0).isUpper && StatArticleTable.table.word_is_toponym(word))
    if (maxword == None) {
      maxword = worddist.find_most_common_word(
        word => word(0).isUpper)
    }
    if (maxword == None)
      maxword = worddist.find_most_common_word(word => true)
    RegionDist.get_region_dist(maxword.get).get_ranked_regions()
  }

  def ranked_regions_link_most_common_toponym(worddist: WordDist) = {
    var maxword = worddist.find_most_common_word(
      word => word(0).isUpper && StatArticleTable.table.word_is_toponym(word))
    if (maxword == None) {
      maxword = worddist.find_most_common_word(
        word => StatArticleTable.table.word_is_toponym(word))
    }
    if (debug("commontop"))
      errprint("  maxword = %s", maxword)
    val cands =
      if (maxword != None)
        StatArticleTable.table.construct_candidates(
          unmemoize_word(maxword.get))
      else Seq[StatArticle]()
    if (debug("commontop"))
      errprint("  candidates = %s", cands)
    // Sort candidate list by number of incoming links
    val candlinks =
      (for (cand <- cands) yield (cand, cand.adjusted_incoming_links.toDouble)).
        // sort by second element of tuple, in reverse order
        sortWith(_._2 > _._2)
    if (debug("commontop"))
      errprint("  sorted candidates = %s", candlinks)

    def find_good_regions_for_coord(cands: Iterable[(StatArticle, Double)]) = {
      for {
        (cand, links) <- candlinks
        val reg = {
          val retval = StatRegion.find_region_for_coord(cand.coord)
          if (retval.latind == None)
            errprint("Strange, found no region for candidate %s", cand)
          retval
        }
        if (reg.latind != None)
      } yield (reg, links)
    }

    // Convert to regions
    val candregs = find_good_regions_for_coord(candlinks)

    if (debug("commontop"))
      errprint("  region candidates = %s", candregs)

    // Return an iterator over all elements in all the given sequences, omitting
    // elements seen more than once and keeping the order.
    def merge_numbered_sequences_uniquely[A, B](seqs: Iterable[(A, B)]*) = {
      val keys_seen = mutable.Set[A]()
      for {
        seq <- seqs
        (s, vall) <- seq
        if (!(keys_seen contains s))
      } yield {
        keys_seen += s
        (s, vall)
      }
    }

    // Append random regions and remove duplicates
    merge_numbered_sequences_uniquely(candregs,
      ranked_regions_random(worddist))
  }

  def return_ranked_regions(worddist: WordDist) = {
    if (baseline_strategy == "link-most-common-toponym")
      ranked_regions_link_most_common_toponym(worddist)
    else if (baseline_strategy == "regdist-most-common-toponym")
      ranked_regions_regdist_most_common_toponym(worddist)
    else if (baseline_strategy == "random")
      ranked_regions_random(worddist)
    else
      ranked_most_popular_regions(worddist)
  }
}

/**
 *  Return ranked regions by scoring each region against the distribution
 *  and returning the regions ordered by score, with the lower the better.
 */

abstract class MinimumScoreStrategy extends GeotagDocumentStrategy {
  def score_region(worddist: WordDist, stat_region: StatRegion): Double

  def return_ranked_regions(worddist: WordDist) = {
    val region_buf = mutable.Buffer[(StatRegion, Double)]()
    for (stat_region <-
           StatRegion.iter_nonempty_regions(nonempty_word_dist = true)) {
      val inds = (stat_region.latind.get, stat_region.longind.get)
      if (debug("lots")) {
        val (latind, longind) = inds
        val coord = region_indices_to_coord(latind, longind)
        errprint("Nonempty region at indices %s,%s = coord %s, num_articles = %s",
          latind, longind, coord, stat_region.worddist.num_arts_for_word_dist)
      }

      val score = score_region(worddist, stat_region)
      region_buf += ((stat_region, score))
    }

    region_buf sortWith (_._2 < _._2)
  }
}

class KLDivergenceStrategy(
  partial: Boolean = true,
  symmetric: Boolean = false) extends MinimumScoreStrategy {

  def score_region(worddist: WordDist, stat_region: StatRegion) = {
    var kldiv = fast_kl_divergence(worddist, stat_region.worddist,
      partial = partial)
    // var kldiv = worddist.test_kl_divergence(stat_region.worddist,
    //  partial = partial)
    if (symmetric) {
      val kldiv2 = fast_kl_divergence(stat_region.worddist, worddist,
        partial = partial)
      kldiv = (kldiv + kldiv2) / 2.0
    }
    //kldiv = worddist.test_kl_divergence(stat_region.worddist,
    //                           partial=partial)
    //errprint("For region %s, KL divergence %.3f", stat_region, kldiv)
    kldiv
  }

  override def return_ranked_regions(worddist: WordDist) = {
    val regions = super.return_ranked_regions(worddist)

    if (debug("kldiv")) {
      // Print out the words that contribute most to the KL divergence, for
      // the top-ranked regions
      val num_contrib_regions = 5
      val num_contrib_words = 25
      errprint("")
      errprint("KL-divergence debugging info:")
      for (i <- 0 until (regions.length min num_contrib_regions)) {
        val (region, _) = regions(i)
        val (_, contribs) =
          worddist.slow_kl_divergence_debug(
            region.worddist, partial = partial,
            return_contributing_words = true)
        errprint("  At rank #%s, region %s:", i + 1, region)
        errprint("    %30s  %s", "Word", "KL-div contribution")
        errprint("    %s", "-" * 50)
        // sort by absolute value of second element of tuple, in reverse order
        val items = (contribs.toArray sortWith ((x, y) => abs(x._2) > abs(y._2))).
          take(num_contrib_words)
        for ((word, contribval) <- items)
          errprint("    %30s  %s", word, contribval)
        errprint("")
      }
    }

    regions
  }
}

class CosineSimilarityStrategy(
  smoothed: Boolean = false,
  partial: Boolean = false) extends MinimumScoreStrategy {

  def score_region(worddist: WordDist, stat_region: StatRegion) = {
    var cossim =
      if (smoothed)
        fast_smoothed_cosine_similarity(worddist, stat_region.worddist,
          partial = partial)
      else
        fast_cosine_similarity(worddist, stat_region.worddist,
          partial = partial)
    assert(cossim >= 0.0)
    // Just in case of round-off problems
    assert(cossim <= 1.002)
    cossim = 1.002 - cossim
    cossim
  }
}

// Return the probability of seeing the given document 
class NaiveBayesDocumentStrategy(
  use_baseline: Boolean = true) extends GeotagDocumentStrategy {

  def return_ranked_regions(worddist: WordDist) = {

    // Determine respective weightings
    val (word_weight, baseline_weight) = (
      if (use_baseline) {
        if (Opts.naive_bayes_weighting == "equal") (1.0, 1.0)
        else {
          val bw = Opts.baseline_weight.toDouble
          ((1.0 - bw) / worddist.total_tokens, bw)
        }
      } else (1.0, 0.0))

    (for {
      reg <- StatRegion.iter_nonempty_regions(nonempty_word_dist = true)
      val word_logprob = reg.worddist.get_nbayes_logprob(worddist)
      val baseline_logprob = log(reg.worddist.num_arts_for_links.toDouble /
        StatRegion.total_num_arts_for_links)
      val logprob = (word_weight * word_logprob +
        baseline_weight * baseline_logprob)
    } yield (reg -> logprob)).toArray.
      // Scala nonsense: sort on the second element of the tuple (foo._2),
      // reserved (_ > _).
      sortWith(_._2 > _._2)
  }
}

class PerWordRegionDistributionsStrategy extends GeotagDocumentStrategy {
  def return_ranked_regions(worddist: WordDist) = {
    val regdist = RegionDist.get_region_dist_for_word_dist(worddist)
    regdist.get_ranked_regions()
  }
}

abstract class GeotagDocumentEvaluator(
  strategy: GeotagDocumentStrategy,
  stratname: String
) extends TestFileEvaluator(stratname) {
  val results = new GeotagDocumentResults()

  // FIXME: Seems strange to have a static function like this called here
  StatRegion.initialize_regions()

  def output_results(isfinal: Boolean = false) {
    results.output_geotag_document_results(all_results = isfinal)
  }
}

class WikipediaGeotagDocumentEvaluator(
  strategy: GeotagDocumentStrategy,
  stratname: String
) extends GeotagDocumentEvaluator(strategy, stratname) {

  type Document = StatArticle

  // Debug flags:
  //
  //  gridrank: For the given test article number (starting at 1), output
  //            a grid of the predicted rank for regions around the true
  //            region.  Multiple articles can have the rank output by
  //            specifying this option multiple times, e.g.
  //
  //            --debug 'gridrank=45,gridrank=58'
  //
  //  gridranksize: Size of the grid, in numbers of articles on a side.
  //                This is a single number, and the grid will be a square
  //                centered on the true region.
  register_list_debug_param("gridrank")
  debugval("gridranksize") = "11"

  def iter_documents(filename: String) = {
    for (art <- StatArticleTable.table.articles_by_split(Opts.eval_set))
      yield art
  }

  //title = None
  //words = []
  //for line in openr(filename, errors="replace"):
  //  if (rematch("Article title: (.*)$", line))
  //    if (title != null)
  //      yield (title, words)
  //    title = m_[1]
  //    words = []
  //  else if (rematch("Link: (.*)$", line))
  //    args = m_[1].split('|')
  //    trueart = args[0]
  //    linkword = trueart
  //    if (len(args) > 1)
  //      linkword = args[1]
  //    words.append(linkword)
  //  else:
  //    words.append(line)
  //if (title != null)
  //  yield (title, words)

  override def would_skip_document(article: StatArticle, doctag: String) = {
    if (article.dist == null) {
      // This can (and does) happen when --max-time-per-stage is set,
      // so that the counts for many articles don't get read in.
      if (Opts.max_time_per_stage == 0 && Opts.num_training_docs == 0)
        warning("Can't evaluate article %s without distribution", article)
      results.record_geotag_document_other_stat("Skipped articles")
      true
    } else false
  }

  def evaluate_document(article: StatArticle, doctag: String): Boolean = {
    if (would_skip_document(article, doctag))
      return false
    assert(article.dist.finished)
    val (true_latind, true_longind) =
      coord_to_stat_region_indices(article.coord)
    val true_statreg = StatRegion.find_region_for_coord(article.coord)
    val naitr = true_statreg.worddist.num_arts_for_word_dist
    if (debug("lots") || debug("commontop"))
      errprint("Evaluating article %s with %s word-dist articles in true region",
        article, naitr)
    val regs = strategy.return_ranked_regions(article.dist).toArray
    var rank = 1
    var broken = false
    breakable {
      for ((reg, value) <- regs) {
        if (reg.latind.get == true_latind && reg.longind.get == true_longind) {
          broken = true
          break
        }
        rank += 1
      }
    }
    if (!broken)
      rank = 1000000000
    val want_indiv_results = !Opts.no_individual_results
    val stats = results.record_geotag_document_result(rank, article.coord,
      regs(0)._1.latind.get, regs(0)._1.longind.get,
      num_arts_in_true_region = naitr,
      return_stats = want_indiv_results)
    if (naitr == 0) {
      results.record_geotag_document_other_stat(
        "Articles with no training articles in region")
    }
    if (want_indiv_results) {
      errprint("%s:Article %s:", doctag, article)
      errprint("%s:  %d types, %d tokens",
        doctag, article.dist.counts.size, article.dist.total_tokens)
      errprint("%s:  true region at rank: %s", doctag, rank)
      errprint("%s:  true region: %s", doctag, true_statreg)
      for (i <- 0 until 5) {
        errprint("%s:  Predicted region (at rank %s): %s",
          doctag, i + 1, regs(i)._1)
      }
      errprint("%s:  Distance %.2f miles to true region center at %s",
        doctag, stats("true_truedist"), stats("true_center"))
      errprint("%s:  Distance %.2f miles to predicted region center at %s",
        doctag, stats("pred_truedist"), stats("pred_center"))
      assert(doctag(0) == '#')
      if (debug("gridrank") ||
        (debuglist("gridrank") contains doctag.drop(1))) {
        val grsize = debugval("gridranksize").toInt
        val min_latind = true_latind - grsize / 2
        val max_latind = min_latind + grsize - 1
        val min_longind = true_longind - grsize / 2
        val max_longind = min_longind + grsize - 1
        val grid = mutable.Map[(Regind, Regind), (StatRegion, Double, Int)]()
        rank = 1
        for ((reg, value) <- regs) {
          val (la, lo) = (reg.latind.get, reg.longind.get)
          if (la >= min_latind && la <= max_latind &&
            lo >= min_longind && lo <= max_longind)
            grid((la, lo)) = (reg, value, rank)
          rank += 1
        }

        errprint("Grid ranking, gridsize %dx%d", grsize, grsize)
        errprint("NW corner: %s",
          stat_region_indices_to_nw_corner_coord(max_latind, min_longind))
        errprint("SE corner: %s",
          stat_region_indices_to_se_corner_coord(min_latind, max_longind))
        for (doit <- Seq(0, 1)) {
          if (doit == 0)
            errprint("Grid for ranking:")
          else
            errprint("Grid for goodness/distance:")
          for (lat <- max_latind to min_latind) {
            for (long <- fromto(min_longind, max_longind)) {
              val regvalrank = grid.getOrElse((lat, long), null)
              if (regvalrank == null)
                errout(" %-8s", "empty")
              else {
                val (reg, vall, rank) = regvalrank
                val showit = if (doit == 0) rank else vall
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

    return true
  }
}

class PCLTravelGeotagDocumentEvaluator(
  strategy: GeotagDocumentStrategy,
  stratname: String
) extends GeotagDocumentEvaluator(strategy, stratname) {
  case class TitledDocument(title: String, text: String)
  type Document = TitledDocument

  def iter_documents(filename: String) = {

    val dom = try {
      // On error, just return, so that we don't have problems when called
      // on the whole PCL corpus dir (which includes non-XML files).
      xml.XML.loadFile(filename)
    } catch {
      case _ => {
        warning("Unable to parse XML filename: %s", filename)
        null
      }
    }

    if (dom == null) Seq[TitledDocument]()
    else for {
      chapter <- dom \\ "div" if (chapter \ "@type").text == "chapter"
      val (heads, nonheads) = chapter.child.partition(_.label == "head")
      val headtext = (for (x <- heads) yield x.text) mkString ""
      val text = (for (x <- nonheads) yield x.text) mkString ""
      //errprint("Head text: %s", headtext)
      //errprint("Non-head text: %s", text)
    } yield TitledDocument(headtext, text)
  }

  def evaluate_document(doc: TitledDocument, doctag: String) = {
    val dist = new WordDist()
    val the_stopwords =
      if (Opts.include_stopwords_in_article_dists) Set[String]()
      else Stopwords.stopwords
    for (text <- Seq(doc.title, doc.text)) {
      dist.add_words(split_text_into_words(text, ignore_punc = true),
        ignore_case = !Opts.preserve_case_words,
        stopwords = the_stopwords)
    }
    dist.finish(minimum_word_count = Opts.minimum_word_count)
    val regs = strategy.return_ranked_regions(dist)
    errprint("")
    errprint("Article with title: %s", doc.title)
    val num_regs_to_show = 5
    for ((rank, regval) <- (1 to num_regs_to_show) zip regs) {
      val (reg, vall) = regval
      if (debug("struct")) {
        errprint("  Rank %d, goodness %g:", rank, vall)
        errprint(reg.struct().toString) // indent=4
      } else
        errprint("  Rank %d, goodness %g: %s", rank, vall, reg.shortstr())
    }

    true
  }
}

/////////////////////////////////////////////////////////////////////////////
//                                Segmentation                             //
/////////////////////////////////////////////////////////////////////////////

// General idea: Keep track of best possible segmentations up to a maximum
// number of segments.  Either do it using a maximum number of segmentations
// (e.g. 100 or 1000) or all within a given factor of the best score (the
// "beam width", e.g. 10^-4).  Then given the existing best segmentations,
// we search for new segmentations with more segments by looking at all
// possible ways of segmenting each of the existing best segments, and
// finding the best score for each of these.  This is a slow process -- for
// each segmentation, we have to iterate over all segments, and for each
// segment we have to look at all possible ways of splitting it, and for
// each split we have to look at all assignments of regions to the two
// new segments.  It also seems that we're likely to consider the same
// segmentation multiple times.
//
// In the case of per-word region dists, we can maybe speed things up by
// computing the non-normalized distributions over each paragraph and then
// summing them up as necessary.

/////////////////////////////////////////////////////////////////////////////
//                                   Stopwords                             //
/////////////////////////////////////////////////////////////////////////////

object Stopwords {
  // List of stopwords
  var stopwords: Set[String] = null

  // Read in the list of stopwords from the given filename.
  def read_stopwords(filename: String) {
    errprint("Reading stopwords from %s...", filename)
    stopwords = openr(filename).toSet
  }
}

/////////////////////////////////////////////////////////////////////////////
//                                  Main code                              //
/////////////////////////////////////////////////////////////////////////////

object Opts {
  val op = new OptionParser("disambig")
  //////////// Input files
  def stopwords_file =
    op.option[String]("stopwords-file",
      metavar = "FILE",
      help = """File containing list of stopwords.""")
  def article_data_file =
    op.multiOption[String]("a", "article-data-file",
      metavar = "FILE",
      help = """File containing info about Wikipedia articles.  Multiple
such files can be given.""")
  def gazetteer_file =
    op.option[String]("gf", "gazetteer-file",
      help = """File containing gazetteer information to match.""")
  def gazetteer_type =
    op.option[String]("gt", "gazetteer-type",
      metavar = "FILE",
      default = "world", choices = Seq("world", "db"),
      help = """Type of gazetteer file specified using --gazetteer;
default '%default'.""")
  def counts_file =
    op.multiOption[String]("counts-file", "cf",
      metavar = "FILE",
      help = """File containing output from a prior run of
--output-counts, listing for each article the words in the article and
associated counts.  Multiple such files can be given.""")
  def eval_file =
    op.option[String]("e", "eval-file",
      metavar = "FILE",
      help = """File or directory containing files to evaluate on.
Each file is read in and then disambiguation is performed.""")
  def eval_format =
    op.option[String]("f", "eval-format",
      default = "wiki",
      choices = Seq("tr-conll", "wiki", "raw-text", "pcl-travel"),
      help = """Format of evaluation file(s).  Default '%default'.""")
  def eval_set =
    op.option[String]("eval-set", "es",
      default = "dev",
      choices = Seq("dev", "test"),
      canonicalize = Map("dev" -> Seq("devel")),
      help = """Set to use for evaluation when --eval-format=wiki
and --mode=geotag-documents ('dev' or 'devel' for the development set,
'test' for the test set).  Default '%default'.""")

  /////////// Misc options for handling distributions
  def opt_preserve_case_words =
    op.flag("preserve-case-words", "pcw",
      help = """Don't fold the case of words used to compute and
match against article distributions.  Note that this does not apply to
toponyms; currently, toponyms are always matched case-insensitively.""")
  var preserve_case_words = false

  def include_stopwords_in_article_dists =
    op.flag("include-stopwords-in-article-dists",
      help = """Include stopwords when computing word
distributions.""")
  def naive_bayes_context_len =
    op.option[Int]("naive-bayes-context-len", "nbcl",
      default = 10,
      help = """Number of words on either side of a toponym to use
in Naive Bayes matching.  Default %default.""")
  def minimum_word_count =
    op.option[Int]("minimum-word-count", "mwc",
      default = 1,
      help = """Minimum count of words to consider in word
distributions.  Words whose count is less than this value are ignored.""")

  /////////// Misc options for controlling matching
  def max_dist_for_close_match =
    op.option[Double]("max-dist-for-close-match", "mdcm",
      default = 80,
      help = """Maximum number of miles allowed when looking for a
close match.  Default %default.""")
  def max_dist_for_outliers =
    op.option[Double]("max-dist-for-outliers", "mdo",
      default = 200,
      help = """Maximum number of miles allowed between a point and
any others in a division.  Points farther away than this are ignored as
"outliers" (possible errors, etc.).  Default %default.""")

  /////////// Basic options for determining operating mode and strategy
  def mode =
    op.option[String]("m", "mode",
      default = "geotag-documents",
      choices = Seq("geotag-toponyms",
        "geotag-documents",
        "generate-kml",
        "segment-geotag-documents"),
      help = """Action to perform.

'geotag-documents' finds the proper location for each document (or article)
in the test set.

'geotag-toponyms' finds the proper location for each toponym in the test set.
The test set is specified by --eval-file.  Default '%default'.

'segment-geotag-documents' simultaneously segments a document into sections
covering a specific location and determines that location. (Not yet
implemented.)

'generate-kml' generates KML files for some set of words, showing the
distribution over regions that the word determines.  Use '--kml-words' to
specify the words whose distributions should be outputted.  See also
'--kml-prefix' to specify the prefix of the files outputted, and
'--kml-transform' to specify the function to use (if any) to transform
the probabilities to make the distinctions among them more visible.
""")

  def opt_strategy =
    op.multiOption[String]("s", "strategy",
      //      choices=Seq(
      //        "baseline", "none",
      //        "full-kl-divergence",
      //        "partial-kl-divergence",
      //        "symmetric-full-kl-divergence",
      //        "symmetric-partial-kl-divergence",
      //        "cosine-similarity",
      //        "partial-cosine-similarity",
      //        "smoothed-cosine-similarity",
      //        "smoothed-partial-cosine-similarity",
      //        "average-cell-probability",
      //        "naive-bayes-with-baseline",
      //        "naive-bayes-no-baseline",
      //        ),
      canonicalize = Map(
        "baseline" -> null, "none" -> null,
        "full-kl-divergence" ->
          Seq("full-kldiv", "full-kl"),
        "partial-kl-divergence" ->
          Seq("partial-kldiv", "partial-kl", "part-kl"),
        "symmetric-full-kl-divergence" ->
          Seq("symmetric-full-kldiv", "symmetric-full-kl", "sym-full-kl"),
        "symmetric-partial-kl-divergence" ->
          Seq("symmetric-partial-kldiv", "symmetric-partial-kl", "sym-part-kl"),
        "cosine-similarity" ->
          Seq("cossim"),
        "partial-cosine-similarity" ->
          Seq("partial-cossim", "part-cossim"),
        "smoothed-cosine-similarity" ->
          Seq("smoothed-cossim"),
        "smoothed-partial-cosine-similarity" ->
          Seq("smoothed-partial-cossim", "smoothed-part-cossim"),
        "average-cell-probability" ->
          Seq("avg-cell-prob", "acp"),
        "naive-bayes-with-baseline" ->
          Seq("nb-base"),
        "naive-bayes-no-baseline" ->
          Seq("nb-nobase")),
      help = """Strategy/strategies to use for geotagging.
'baseline' means just use the baseline strategy (see --baseline-strategy).

'none' means don't do any geotagging.  Useful for testing the parts that
read in data and generate internal structures.

The other possible values depend on which mode is in use
(--mode=geotag-toponyms or --mode=geotag-documents).

For geotag-toponyms:

'naive-bayes-with-baseline' (or 'nb-base') means also use the words around the
toponym to be disambiguated, in a Naive-Bayes scheme, using the baseline as the
prior probability; 'naive-bayes-no-baseline' (or 'nb-nobase') means use uniform
prior probability.  Default is 'baseline'.

For geotag-documents:

'full-kl-divergence' (or 'full-kldiv') searches for the region where the KL
divergence between the article and region is smallest.
'partial-kl-divergence' (or 'partial-kldiv') is similar but uses an
abbreviated KL divergence measure that only considers the words seen in the
article; empirically, this appears to work just as well as the full KL
divergence. 'average-cell-probability' (or
'regdist') involves computing, for each word, a probability distribution over
regions using the word distribution of each region, and then combining the
distributions over all words in an article, weighted by the count the word in
the article.  Default is 'partial-kl-divergence'.

NOTE: Multiple --strategy options can be given, and each strategy will
be tried, one after the other.""")
  var strategy: Seq[String] = null

  def opt_baseline_strategy =
    op.multiOption[String]("baseline-strategy", "bs",
      choices = Seq("internal-link", "random",
        "num-articles", "link-most-common-toponym",
        "region-distribution-most-common-toponym"),
      canonicalize = Map(
        "internal-link" -> Seq("link"),
        "num-articles" -> Seq("num-arts", "numarts"),
        "region-distribution-most-common-toponym" ->
          Seq("regdist-most-common-toponym")),
      help = """Strategy to use to compute the baseline.

'internal-link' (or 'link') means use number of internal links pointing to the
article or region.

'random' means choose randomly.

'num-articles' (or 'num-arts' or 'numarts'; only in region-type matching) means
use number of articles in region.

'link-most-common-toponym' (only in --mode=geotag-documents) means to look
for the toponym that occurs the most number of times in the article, and
then use the internal-link baseline to match it to a location.

'regdist-most-common-toponym' (only in --mode=geotag-documents) is similar,
but uses the region distribution of the most common toponym.

Default '%default'.

NOTE: Multiple --baseline-strategy options can be given, and each strategy will
be tried, one after the other.  Currently, however, the *-most-common-toponym
strategies cannot be mixed with other baseline strategies, or with non-baseline
strategies, since they require that --preserve-case-words be set internally.""")
  var baseline_strategy: Seq[String] = null

  def baseline_weight =
    op.option[Double]("baseline-weight", "bw",
      metavar = "WEIGHT",
      default = 0.5,
      help = """Relative weight to assign to the baseline (prior
probability) when doing weighted Naive Bayes.  Default %default.""")
  def naive_bayes_weighting =
    op.option[String]("naive-bayes-weighting", "nbw",
      default = "equal",
      choices = Seq("equal", "equal-words", "distance-weighted"),
      help = """Strategy for weighting the different probabilities
that go into Naive Bayes.  If 'equal', do pure Naive Bayes, weighting the
prior probability (baseline) and all word probabilities the same.  If
'equal-words', weight all the words the same but collectively weight all words
against the baseline, giving the baseline weight according to --baseline-weight
and assigning the remainder to the words.  If 'distance-weighted', similar to
'equal-words' but don't weight each word the same as each other word; instead,
weight the words according to distance from the toponym.""")
  def width_of_stat_region =
    op.option[Int]("width-of-stat-region", default = 1,
      help = """Width of the region used to compute a statistical
distribution for geotagging purposes, in terms of number of tiling regions.
Default %default.""")
  def degrees_per_region =
    op.option[Double]("degrees-per-region", "dpr",
      help = """Size (in degrees) of the tiling regions that cover
the earth.  Some number of tiling regions are put together to form the region
used to construct a statistical distribution.  No default; the default of
'--miles-per-region' is used instead.""")
  def miles_per_region =
    op.option[Double]("miles-per-region", "mpr",
      default = 100.0,
      help = """Size (in miles) of the tiling regions that cover
the earth.  Some number of tiling regions are put together to form the region
used to construct a statistical distribution.  Default %default.""")
  def context_type =
    op.option[String]("context-type", "ct",
      default = "region-dist-article-links",
      choices = Seq("article", "region", "region-dist-article-links"),
      help = """Type of context used when doing disambiguation.
There are two cases where this choice applies: When computing a word
distribution, and when counting the number of incoming internal links.
'article' means use the article itself for both.  'region' means use the
region for both. 'region-dist-article-links' means use the region for
computing a word distribution, but the article for counting the number of
incoming internal links.  Note that this only applies when
--mode='geotag-toponyms'; in --mode='geotag-documents', only regions are
considered.  Default '%default'.""")

  def kml_words =
    op.option[String]("k", "kml-words", "kw",
      help = """Words to generate KML distributions for, when
--mode='generate-kml'.  Each word should be separated by a comma.  A separate
file is generated for each word, using '--kml-prefix' and adding '.kml'.""")
  def kml_prefix =
    op.option[String]("kml-prefix", "kp",
      default = "kml-dist.",
      help = """Prefix to use for KML files outputted.
Default '%default',""")
  def kml_transform =
    op.option[String]("kml-transform", "kt", "kx",
      default = "none",
      choices = Seq("none", "log", "logsquared"),
      help = """Type of transformation to apply to the probabilities
when generating KML, possibly to try and make the low values more visible.
Possibilities are 'none' (no transformation), 'log' (take the log), and
'logsquared' (negative of squared log).  Default '%default'.""")

  def num_training_docs =
    op.option[Int]("num-training-docs", "ntrain", default = 0,
      help = """Maximum number of training documents to use.
0 means no limit.  Default %default.""")
  def num_test_docs =
    op.option[Int]("num-test-docs", "ntest", default = 0,
      help = """Maximum number of test documents to process.
0 means no limit.  Default %default.""")
  def skip_initial_test_docs =
    op.option[Int]("skip-initial-test-docs", "skip-initial", default = 0,
      help = """Skip this many test docs at beginning.  Default 0.""")
  def every_nth_test_doc =
    op.option[Int]("every-nth-test-doc", "every-nth", default = 1,
      help = """Only process every Nth test doc.  Default 1, i.e. process all.""")
  //  def skip_every_n_test_docs =
  //    op.option[Int]("skip-every-n-test-docs", "skip-n", default=0,
  //      help="""Skip this many after each one processed.  Default 0.""")
  def no_individual_results =
    op.flag("no-individual-results", "no-results",
      help = """Don't show individual results for each test document.""")
  def lru_cache_size =
    op.option[Int]("lru-cache-size", "lru", default = 400,
      help = """Number of entries in the LRU cache.""")

  // Shared options in old code
  def max_time_per_stage =
    op.option[Int]("max-time-per-stage", "mts", default = 0,
      help = """Maximum time per stage in seconds.  If 0, no limit.
  Used for testing purposes.  Default %default.""")
  def debug =
    op.option[String]("d", "debug", metavar = "FLAGS",
      help = "Output debug info of the given types (separated by spaces or commas)")
}

object Disambig extends NlpProgram {
  val opts = Opts
  val op = Opts.op

  var need_to_read_stopwords = false

  override def output_parameters() {
    errprint("Need to read stopwords: %s", need_to_read_stopwords)
  }

  def handle_arguments(op: OptionParser, args: Seq[String]) {
    if (Opts.debug != null) {
      val params = """[:;\s]+""".r.split(Opts.debug)
      // Allow params with values, and allow lists of values to be given
      // by repeating the param
      for (f <- params) {
        if (f contains '=') {
          val Array(param, value) = f.split("=", 2)
          if (list_debug_params contains param) {
            val values = "[,]".split(value)
            debuglist(param) ++= values
          } else
            debugval(param) = value
        } else
          debug(f) = true
      }
    }

    // Canonicalize options
    Opts.strategy = Opts.opt_strategy
    Opts.baseline_strategy = Opts.opt_baseline_strategy
    Opts.preserve_case_words = Opts.opt_preserve_case_words

    if (Opts.strategy.length == 0) {
      if (Opts.mode == "geotag-documents")
        Opts.strategy = Seq("partial-kl-divergence")
      else if (Opts.mode == "geotag-toponyms")
        Opts.strategy = Seq("baseline")
      else
        Opts.strategy = Seq[String]()
    }

    if (Opts.baseline_strategy.length == 0)
      Opts.baseline_strategy = Seq("internal-link")

    if (Opts.strategy contains "baseline") {
      var need_case = false
      var need_no_case = false
      for (bstrat <- Opts.baseline_strategy) {
        if (bstrat.endsWith("most-common-toponym"))
          need_case = true
        else
          need_no_case = true
      }
      if (need_case) {
        if (Opts.strategy.length > 1 || need_no_case) {
          // That's because we have to set --preserve-case-words, which we
          // generally don't want set for other strategies and which affects
          // the way we construct the training-document distributions.
          op.error("Can't currently mix *-most-common-toponym baseline strategy with other strategies")
        }
        Opts.preserve_case_words = true
      }
    }

    // FIXME! Can only currently handle World-type gazetteers.
    if (Opts.gazetteer_type != "world")
      op.error("Currently can only handle world-type gazetteers")

    if (Opts.miles_per_region <= 0)
      op.error("Miles per region must be positive")
    Distances.degrees_per_region =
      if (Opts.degrees_per_region > 0) Opts.degrees_per_region
      else Opts.miles_per_region / miles_per_degree
    // The actual maximum latitude is exactly 90 (the North Pole).  But if we
    // set degrees per region to be a number that exactly divides 180, and we
    // use maximum_latitude = 90 in the following computations, then we would
    // end up with the North Pole in a region by itself, something we probably
    // don't want.
    val (maxlatind, maxlongind) =
      coord_to_tiling_region_indices(Coord(maximum_latitude - 1e-10,
        maximum_longitude))
    Distances.maximum_latind = maxlatind
    Distances.maximum_longind = maxlongind
    val (minlatind, minlongind) =
      coord_to_tiling_region_indices(Coord(minimum_latitude,
        minimum_longitude))
    Distances.minimum_latind = minlatind
    Distances.minimum_longind = minlongind

    if (Opts.width_of_stat_region <= 0)
      op.error("Width of statistical region must be positive")
    Distances.width_of_stat_region = Opts.width_of_stat_region

    //// Start reading in the files and operating on them ////

    if (Opts.mode.startsWith("geotag")) {
      need_to_read_stopwords = true
      if (Opts.mode == "geotag-toponyms" && Opts.strategy == Seq("baseline"))
        ()
      else if (Opts.counts_file == null)
        op.error("Must specify counts file")
    }

    if (Opts.mode == "geotag-toponyms")
      need("gazetteer-file")

    if (Opts.eval_format == "raw-text") {
      // FIXME!!!!
      op.error("Raw-text reading not implemented yet")
    }

    if (Opts.mode == "geotag-documents") {
      if (!(Seq("pcl-travel", "wiki") contains Opts.eval_format))
        op.error("For --mode=geotag-documents, eval-format must be 'pcl-travel' or 'wiki'")
    } else if (Opts.mode == "geotag-toponyms") {
      if (Opts.baseline_strategy.endsWith("most-common-toponym")) {
        op.error("--baseline-strategy=%s only compatible with --mode=geotag-documents"
          format Opts.baseline_strategy)
      }
      for (stratname <- Opts.strategy) {
        if (!(Seq("baseline", "naive-bayes-with-baseline",
          "naive-bayes-no-baseline") contains stratname)) {
          op.error("Strategy '%s' invalid for --mode=geotag-toponyms" format
            stratname)
        }
      }
      if (!(Seq("tr-conll", "wiki") contains Opts.eval_format))
        op.error("For --mode=geotag-toponyms, eval-format must be 'tr-conll' or 'wiki'")
    }

    if (Opts.mode == "geotag-documents" && Opts.eval_format == "wiki")
      () // No need for evaluation file, uses the counts file
    else if (Opts.mode.startsWith("geotag"))
      need("eval-file", "evaluation file(s)")

    if (Opts.mode == "generate-kml")
      need("kml-words")
    else if (Opts.kml_words != null)
      op.error("--kml-words only compatible with --mode=generate-kml")

    need("article-data-file")
  }

  def implement_main(op: OptionParser, args: Seq[String]) {
    import Toponym._

    if (need_to_read_stopwords)
      Stopwords.read_stopwords(Opts.stopwords_file)

    val table =
    if (Opts.mode == "geotag-toponyms") {
        TopoArticleTable.table = new TopoArticleTable()
        TopoArticleTable.table
      } else
        new StatArticleTable()
    StatArticleTable.table = table

    for (fn <- Opts.article_data_file)
      table.read_article_data(fn)

    // errprint("Processing evaluation file(s) %s for toponym counts...",
    //   Opts.eval_file)
    // process_dir_files(Opts.eval_file, count_toponyms_in_file)
    // errprint("Number of toponyms seen: %s",
    //   toponyms_seen_in_eval_files.length)
    // errprint("Number of toponyms seen more than once: %s",
    //   (for {(foo,count) <- toponyms_seen_in_eval_files
    //             if (count > 1)} yield foo).length)
    // output_reverse_sorted_table(toponyms_seen_in_eval_files,
    //                             outfile=sys.stderr)

    // Read in the words-counts file
    if (Opts.counts_file.length > 0) {
      for (fn <- Opts.counts_file)
        table.read_word_counts(fn)
      table.finish_word_counts()
    }

    if (Opts.gazetteer_file != null)
      Gazetteer.gazetteer =
        new WorldGazetteer(Opts.gazetteer_file)

    if (Opts.mode == "generate-kml") {
      StatRegion.initialize_regions()
      val words = Opts.kml_words.split(',')
      for (word <- words) {
        val regdist = RegionDist.get_region_dist(memoize_word(word))
        if (!regdist.normalized) {
          warning("""Non-normalized distribution, apparently word %s not seen anywhere.
Not generating an empty KML file.""", word)
        } else
          regdist.generate_kml_file("%s%s.kml" format (Opts.kml_prefix, word))
      }
      return
    }

    def process_strategies[T](
      strat_unflat: Seq[Seq[(String, T)]])(
      geneval: (String, T) => TestFileEvaluator) {
      val strats = strat_unflat reduce (_ ++ _)
      for ((stratname, strategy) <- strats) {
        val evalobj = geneval(stratname, strategy)
        errprint("Processing evaluation file/dir %s...", Opts.eval_file)
        val iterfiles =
          if (Opts.eval_file != null) iter_directory_files(Opts.eval_file)
          else Seq("foo")
        evalobj.evaluate_and_output_results(iterfiles)
      }
    }

    if (Opts.mode == "geotag-toponyms") {
      val strats = (
        for (stratname <- Opts.strategy) yield {
          // Generate strategy object
          if (stratname == "baseline") {
            for (basestratname <- Opts.baseline_strategy) yield ("baseline " + basestratname,
              new BaselineGeotagToponymStrategy(basestratname))
          } else {
            val strategy = new NaiveBayesToponymStrategy(
              use_baseline = (stratname == "naive-bayes-with-baseline"))
            Seq((stratname, strategy))
          }
        })
      process_strategies(strats)((stratname, strategy) => {
        // Generate reader object
        if (Opts.eval_format == "tr-conll")
          new TRCoNLLGeotagToponymEvaluator(strategy, stratname)
        else
          new WikipediaGeotagToponymEvaluator(strategy, stratname)
      })
    } else if (Opts.mode == "geotag-documents") {
      val strats = (
        for (stratname <- Opts.strategy) yield {
          if (stratname == "baseline") {
            for (basestratname <- Opts.baseline_strategy) yield ("baseline " + basestratname,
              new BaselineGeotagDocumentStrategy(basestratname))
          } else {
            val strategy =
              if (stratname.startsWith("naive-bayes-"))
                new NaiveBayesDocumentStrategy(
                  use_baseline = (stratname == "naive-bayes-with-baseline"))
              else stratname match {
                case "average-cell-probability" =>
                  new PerWordRegionDistributionsStrategy()
                case "cosine-similarity" =>
                  new CosineSimilarityStrategy(smoothed = false, partial = false)
                case "partial-cosine-similarity" =>
                  new CosineSimilarityStrategy(smoothed = false, partial = true)
                case "smoothed-cosine-similarity" =>
                  new CosineSimilarityStrategy(smoothed = true, partial = false)
                case "smoothed-partial-cosine-similarity" =>
                  new CosineSimilarityStrategy(smoothed = true, partial = true)
                case "full-kl-divergence" =>
                  new KLDivergenceStrategy(symmetric = false, partial = false)
                case "partial-kl-divergence" =>
                  new KLDivergenceStrategy(symmetric = false, partial = true)
                case "symmetric-full-kl-divergence" =>
                  new KLDivergenceStrategy(symmetric = true, partial = false)
                case "symmetric-partial-kl-divergence" =>
                  new KLDivergenceStrategy(symmetric = true, partial = true)
                case "none" =>
                  null
              }
            if (strategy != null)
              Seq((stratname, strategy))
            else
              Seq()
          }
        })
      process_strategies(strats)((stratname, strategy) => {
        // Generate reader object
        if (Opts.eval_format == "pcl-travel")
          new PCLTravelGeotagDocumentEvaluator(strategy, stratname)
        else
          new WikipediaGeotagDocumentEvaluator(strategy, stratname)
      })
    }
  }

  main()
}

