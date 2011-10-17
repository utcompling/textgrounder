package opennlp.textgrounder.geolocate

import NlpUtil._
import Distances._
import Debug._

import collection.mutable
import util.control.Breaks._
import math._

object Toponym {

  // A class holding the boundary of a geographic object.  Currently this is
  // just a bounding box, but eventually may be expanded to including a
  // convex hull or more complex model.

  class Boundary(botleft: Coord, topright: Coord) {
    override def toString() = {
      "%s-%s" format (botleft, topright)
    }

    // def __repr__()  = {
    //   "Boundary(%s)" format toString()
    // }

    def struct() = <Boundary boundary={ "%s-%s" format (botleft, topright) }/>

    def contains(coord: Coord) = {
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

    def square_area() = {
      var (lat1, lon1) = (botleft.lat, botleft.long)
      var (lat2, lon2) = (topright.lat, topright.long)
      lat1 = (lat1 / 180.) * Pi
      lat2 = (lat2 / 180.) * Pi
      lon1 = (lon1 / 180.) * Pi
      lon2 = (lon2 / 180.) * Pi

      (earth_radius_in_miles * earth_radius_in_miles) *
        abs(sin(lat1) - sin(lat2)) *
        abs(lon1 - lon2)
    }

    // Iterate over the regions that overlap the boundary.  If
    // 'nonempty_word_dist' is true, only yield regions with a non-empty
    // word distribution; else, yield all non-empty regions.
    def iter_nonempty_tiling_regions() = {
      val (latind1, longind1) = coord_to_tiling_region_indices(botleft)
      val (latind2, longind2) = coord_to_tiling_region_indices(topright)
      for {
        i <- latind1 to latind2 view
        val it = if (longind1 <= longind2) longind1 to longind2 view
        else (longind1 to maximum_longind view) ++
          (minimum_longind to longind2 view)
        j <- it
        if (StatRegion.tiling_region_to_articles contains ((i, j)))
      } yield (i, j)
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
  //   artmatch: Wikipedia article corresponding to this location.
  //   div: Next higher-level division this location is within, or None.

  abstract class Location(
    val name: String,
    val altnames: Seq[String],
    val typ: String) {
    var artmatch: StatArticle = null
    var div: Division = null
    def toString(no_article: Boolean = false): String
    def shortstr(): String
    def struct(no_article: Boolean = false): xml.Elem
    def distance_to_coord(coord: Coord): Double
    def matches_coord(coord: Coord): Boolean
  }

  // A location corresponding to an entry in a gazetteer, with a single
  // coordinate.
  //
  // The following fields are defined, in addition to those for Location:
  //
  //   coord: Coordinates of the location, as a Coord object.
  //   stat_region: The statistical region surrounding this location, including
  //             all necessary information to determine the region-based
  //             distribution.

  case class Locality(
    override val name: String,
    val coord: Coord,
    override val altnames: Seq[String],
    override val typ: String) extends Location(name, altnames, typ) {
    var stat_region: StatRegion = null

    def toString(no_article: Boolean = false) = {
      var artmatch = ""
      if (!no_article)
        artmatch = ", match=%s" format artmatch
      "Locality %s (%s) at %s%s" format (
        name, if (div != null) div.path.mkString("/") else "unknown",
        coord, artmatch)
    }

    // def __repr__() = {
    //   toString.encode("utf-8")
    // }

    def shortstr() = {
      "Locality %s (%s)" format (
        name, if (div != null) div.path.mkString("/") else "unknown")
    }

    def struct(no_article: Boolean = false) =
      <Locality>
        <name>{ name }</name>
        <inDivision>{ if (div != null) div.path.mkString("/") else "" }</inDivision>
        <atCoordinate>{ coord }</atCoordinate>
        {
          if (!no_article)
            <matching>{ if (artmatch != null) artmatch.struct() else "none" }</matching>
        }
      </Locality>

    def distance_to_coord(coord: Coord) = spheredist(coord, coord)

    def matches_coord(coord: Coord) = {
      distance_to_coord(coord) <= Opts.max_dist_for_close_match
    }
  }

  // A division higher than a single locality.  According to the World
  // gazetteer, there are three levels of divisions.  For the U.S., this
  // corresponds to country, state, county.
  //
  case class Division(
    // Tuple of same size as the level #, listing the path of divisions
    // from highest to lowest, leading to this division.  The last
    // element is the same as the "name" of the division.
    path: Seq[String]
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
    // region (e.g. set of convex regions).
    var boundary: Boundary = null
    // For region-based Naive Bayes disambiguation, a distribution
    // over the division's article and all locations within the region.
    var worddist: RegionWordDist = null

    def toString(no_article: Boolean = false) = {
      val artmatchstr =
        if (no_article) "" else ", match=%s" format artmatch
      "Division %s (%s)%s, boundary=%s" format (
        name, path.mkString("/"), artmatchstr, boundary)
    }

    // def __repr__() = toString.encode("utf-8")

    def shortstr() = {
      ("Division %s" format name) + (
        if (level > 1) " (%s)" format (path.mkString("/")) else "")
    }

    def struct(no_article: Boolean = false): xml.Elem =
      <Division>
        <name>{ name }</name>
        <path>{ path.mkString("/") }</path>
        {
          if (!no_article)
            <matching>{ if (artmatch != null) artmatch.struct() else "none" }</matching>
        }
        <boundary>{ boundary.struct() }</boundary>
      </Division>

    def distance_to_coord(coord: Coord) = java.lang.Double.NaN

    def matches_coord(coord: Coord) = this contains coord

    // Compute the boundary of the geographic region of this division, based
    // on the points in the region.
    def compute_boundary() {
      // Yield up all points that are not "outliers", where outliers are defined
      // as points that are more than Opts.max_dist_for_outliers away from all
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
          //  if (mindist <= Opts.max_dist_for_outliers)
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
      val topleft = Coord((for (x <- goodlocs) yield x.coord.lat) min,
        (for (x <- goodlocs) yield x.coord.long) min)
      val botright = Coord((for (x <- goodlocs) yield x.coord.lat) max,
        (for (x <- goodlocs) yield x.coord.long) max)
      boundary = new Boundary(topleft, botright)
    }

    def generate_worddist() {
      worddist = new RegionWordDist()
      val arts =
        for (loc <- Seq(this) ++ goodlocs if loc.artmatch != null)
          yield loc.artmatch
      worddist.add_articles(arts)
      worddist.finish(minimum_word_count = Opts.minimum_word_count)
    }

    def contains(coord: Coord) = boundary contains coord
  }

  object Division {
    // For each division, map from division's path to Division object.
    val path_to_division = mutable.Map[Seq[String], Division]()

    // For each tiling region, list of divisions that have territory in it
    val tiling_region_to_divisions = bufmap[(Regind, Regind), Division]()

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
            Gazetteer.gazetteer.record_division(path.last.toLowerCase, newdiv)
            newdiv
          }
        }
        division.locs += loc
        division
      }
    }

    // Finish all computations related to Divisions, after we've processed
    // all points (and hence all points have been added to the appropriate
    // Divisions).
    def finish_all() {
      val divs_by_area = mutable.Buffer[(Division, Double)]()
      for (division <- path_to_division.values) {
        if (debug("lots")) {
          errprint("Processing division named %s, path %s",
            division.name, division.path)
        }
        division.compute_boundary()
        val artmatch = TopoArticleTable.table.find_match_for_division(division)
        if (artmatch != null) {
          if (debug("lots")) {
            errprint("Matched article %s for division %s, path %s",
              artmatch, division.name, division.path)
          }
          division.artmatch = artmatch
          artmatch.location = division
        } else {
          if (debug("lots")) {
            errprint("Couldn't find match for division %s, path %s",
              division.name, division.path)
          }
        }
        for (inds <- division.boundary.iter_nonempty_tiling_regions())
          tiling_region_to_divisions(inds) += division
        if (debug("region"))
          divs_by_area += ((division, division.boundary.square_area()))
      }
      if (debug("region")) {
        // sort by second element of tuple, in reverse order
        for ((div, area) <- divs_by_area sortWith (_._2 > _._2))
          errprint("%.2f square miles: %s", area, div)
      }
    }
  }

  // A Wikipedia article for toponym resolution.

  class TopoArticle(params: Map[String, String]) extends StatArticle(params) {
    // StatRegion object corresponding to this article.
    var stat_region: StatRegion = null
    // Corresponding location for this article.
    var location: Location = null

    override def toString() = {
      var ret = super.toString()
      if (location != null) {
        ret += (", matching location %s" format
          location.toString(no_article = true))
      }
      val divs = find_covering_divisions()
      val top_divs =
        for (div <- divs if div.level == 1)
          yield div.toString(no_article = true)
      val topdivstr =
      if (top_divs.length > 0)
          ", in top-level divisions %s" format (top_divs.mkString(", "))
        else
          ", not in any top-level divisions"
      ret + topdivstr
    }

    override def shortstr() = {
      var str = super.shortstr()
      if (location != null)
        str += ", matching %s" format location.shortstr()
      val divs = find_covering_divisions()
      val top_divs = (for (div <- divs if div.level == 1) yield div.name)
      if (top_divs.length > 0)
        str += ", in top-level divisions %s" format (top_divs.mkString(", "))
      str
    }

    override def struct() = {
      val xml = super.struct()
      <TopoArticle>
        { xml.child }
        {
          if (location != null)
            <matching>{ location.struct(no_article = true) }</matching>
        }
        {
          val divs = find_covering_divisions()
          val top_divs = (for (div <- divs if div.level == 1)
            yield div.struct(no_article = true))
          if (top_divs != null)
            <topLevelDivisions>{ top_divs }</topLevelDivisions>
          else
            <topLevelDivisions>none</topLevelDivisions>
        }
      </TopoArticle>
    }

    def matches_coord(coord: Coord) = {
      if (distance_to_coord(coord) <= Opts.max_dist_for_close_match) true
      else if (location != null && location.isInstanceOf[Division] &&
        location.matches_coord(coord)) true
      else false
    }

    // Determine the region word-distribution object for a given article:
    // Create and populate one if necessary.
    def find_regworddist() = {
      val loc = location
      if (loc != null && loc.isInstanceOf[Division]) {
        val div = loc.asInstanceOf[Division]
        if (div.worddist == null)
          div.generate_worddist()
        div.worddist
      } else {
        if (stat_region == null)
          stat_region = StatRegion.find_region_for_coord(coord)
        stat_region.worddist
      }
    }

    // Find the divisions that cover the given article.
    def find_covering_divisions() = {
      val inds = coord_to_tiling_region_indices(coord)
      val divs = Division.tiling_region_to_divisions(inds)
      (for (div <- divs if div contains coord) yield div)
    }
  }

  // Static class maintaining additional tables listing mapping between
  // names, ID's and articles.  See comments at StatArticleTable.
  class TopoArticleTable extends StatArticleTable {
    override def create_article(params: Map[String, String]) =
      new TopoArticle(params)

    // Construct the list of possible candidate articles for a given toponym
    override def construct_candidates(toponym: String) = {
      val lotop = toponym.toLowerCase
      val locs = (
        Gazetteer.gazetteer.lower_toponym_to_location(lotop) ++
        Gazetteer.gazetteer.lower_toponym_to_division(lotop))
      val articles = super.construct_candidates(toponym)
      articles ++ (
        for {loc <- locs
             if (loc.artmatch != null && !(articles contains loc.artmatch))}
          yield loc.artmatch
      )
    }

    override def word_is_toponym(word: String) = {
      val lw = word.toLowerCase
      (super.word_is_toponym(word) ||
        (Gazetteer.gazetteer.lower_toponym_to_location contains lw) ||
        (Gazetteer.gazetteer.lower_toponym_to_division contains lw))
    }

    // Find Wikipedia article matching name NAME for location LOC.  NAME
    // will generally be one of the names of LOC (either its canonical
    // name or one of the alternate name).  CHECK_MATCH is a function that
    // is passed one arument, the Wikipedia article,
    // and should return true if the location matches the article.
    // PREFER_MATCH is used when two or more articles match.  It is passed
    // two arguments, the two Wikipedia articles.  It
    // should return TRUE if the first is to be preferred to the second.
    // Return the article matched, or None.

    def find_one_wikipedia_match(loc: Location, name: String,
      check_match: (StatArticle) => Boolean,
      prefer_match: (StatArticle, StatArticle) => Boolean): StatArticle = {

      val loname = name.toLowerCase

      // Look for any articles with same name (case-insensitive) as the
      // location, check for matches
      for (art <- lower_name_to_articles(loname))
        if (check_match(art)) return art

      // Check whether there is a match for an article whose name is
      // a combination of the location's name and one of the divisions that
      // the location is in (e.g. "Augusta, Georgia" for a location named
      // "Augusta" in a second-level division "Georgia").
      if (loc.div != null) {
        for {
          div <- loc.div.path
          art <- lower_name_div_to_articles((loname, div.toLowerCase))
        } if (check_match(art)) return art
      }

      // See if there is a match with any of the articles whose short name
      // is the same as the location's name
      val arts = short_lower_name_to_articles(loname)
      if (arts != null) {
        val goodarts = (for (art <- arts if check_match(art)) yield art)
        if (goodarts.length == 1)
          return goodarts(0) // One match
        else if (goodarts.length > 1) {
          // Multiple matches: Sort by preference, return most preferred one
          if (debug("lots")) {
            errprint("Warning: Saw %s toponym matches: %s",
              goodarts.length, goodarts)
          }
          val sortedarts = goodarts sortWith (prefer_match(_, _))
          return sortedarts(0)
        }
      }

      // No match.
      return null
    }

    // Find Wikipedia article matching location LOC.  CHECK_MATCH and
    // PREFER_MATCH are as above.  Return the article matched, or None.

    def find_wikipedia_match(loc: Location,
      check_match: (StatArticle) => Boolean,
      prefer_match: (StatArticle, StatArticle) => Boolean): StatArticle = {
      // Try to find a match for the canonical name of the location
      val artmatch = find_one_wikipedia_match(loc, loc.name, check_match,
        prefer_match)
      if (artmatch != null) return artmatch

      // No match; try each of the alternate names in turn.
      for (altname <- loc.altnames) {
        val artmatch2 = find_one_wikipedia_match(loc, altname, check_match,
          prefer_match)
        if (artmatch2 != null) return artmatch2
      }

      // No match.
      return null
    }

    // Find Wikipedia article matching locality LOC; the two coordinates must
    // be at most MAXDIST away from each other.

    def find_match_for_locality(loc: Locality, maxdist: Double) = {

      def check_match(art: StatArticle) = {
        val dist = spheredist(loc.coord, art.coord)
        if (dist <= maxdist) true
        else {
          if (debug("lots")) {
            errprint("Found article %s but dist %s > %s",
              art, dist, maxdist)
          }
          false
        }
      }

      def prefer_match(art1: StatArticle, art2: StatArticle) = {
        spheredist(loc.coord, art1.coord) < spheredist(loc.coord, art2.coord)
      }

      find_wikipedia_match(loc, check_match, prefer_match).
        asInstanceOf[TopoArticle]
    }

    // Find Wikipedia article matching division DIV; the article coordinate
    // must be inside of the division's boundaries.

    def find_match_for_division(div: Division) = {

      def check_match(art: StatArticle) = {
        if (art.coord != null && (div contains art.coord)) true
        else {
          if (debug("lots")) {
            if (art.coord == null) {
              errprint("Found article %s but no coordinate, so not in location named %s, path %s",
                art, div.name, div.path)
            } else {
              errprint("Found article %s but not in location named %s, path %s",
                art, div.name, div.path)
            }
          }
          false
        }
      }

      def prefer_match(art1: StatArticle, art2: StatArticle) = {
        val l1 = art1.incoming_links
        val l2 = art2.incoming_links
        // Prefer according to incoming link counts, if that info is available
        if (l1 != None && l2 != None) l1.get > l2.get
        else {
          // FIXME: Do something smart here -- maybe check that location is
          // farther in the middle of the bounding box (does this even make
          // sense???)
          true
        }
      }

      find_wikipedia_match(div, check_match, prefer_match).
        asInstanceOf[TopoArticle]
    }
  }

  object TopoArticleTable {
    // Currently only one TopoArticleTable object.  This is the same as
    // the object in StatArticleTable.table but is of a different type
    // (a subclass), for easier access.
    var table: TopoArticleTable = null
  }

  class EvalWithCandidateList(
    incorrect_reasons: Map[String, String],
    max_individual_candidates: Int = 5) extends Eval(incorrect_reasons) {
    // Toponyms by number of candidates available
    val total_instances_by_num_candidates = intmap[Int]()
    val correct_instances_by_num_candidates = intmap[Int]()
    val incorrect_instances_by_num_candidates = intmap[Int]()

    def record_result(correct: Boolean, reason: String, num_candidates: Int) {
      super.record_result(correct, reason)
      total_instances_by_num_candidates(num_candidates) += 1
      if (correct)
        correct_instances_by_num_candidates(num_candidates) += 1
      else
        incorrect_instances_by_num_candidates(num_candidates) += 1
    }

    // SCALABUG: The need to write collection.Map here rather than simply
    // Map seems clearly wrong.  It seems the height of obscurity that
    // "collection.Map" is the common supertype of plain "Map"; the use of
    // overloaded "Map" seems to be the root of the problem.
    def output_table_by_num_candidates(table: collection.Map[Int, Int],
      total: Int) {
      for (i <- 0 to max_individual_candidates)
        output_fraction("  With %d  candidates" format i, table(i), total)
      val items = (
        for ((key, value) <- table if key > max_individual_candidates)
          yield value).sum
      output_fraction(
        "  With %d+ candidates" format (1 + max_individual_candidates),
        items, total)
    }

    override def output_correct_results() {
      super.output_correct_results()
      output_table_by_num_candidates(
        correct_instances_by_num_candidates, correct_instances)
    }

    override def output_incorrect_results() {
      super.output_incorrect_results()
      output_table_by_num_candidates(
        incorrect_instances_by_num_candidates, incorrect_instances)
    }
  }

  object GeotagToponymResults {
    val incorrect_geotag_toponym_reasons = Map(
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

  //////// Results for geotagging toponyms
  class GeotagToponymResults {
    import GeotagToponymResults._

    // Overall statistics
    val all_toponym = new EvalWithCandidateList(incorrect_geotag_toponym_reasons)
    // Statistics when toponym not same as true name of location
    val diff_surface = new EvalWithCandidateList(incorrect_geotag_toponym_reasons)
    // Statistics when toponym not same as true name or short form of location
    val diff_short = new EvalWithCandidateList(incorrect_geotag_toponym_reasons)

    def record_geotag_toponym_result(correct: Boolean, toponym: String,
        trueloc: String, reason: String, num_candidates: Int) {
      all_toponym.record_result(correct, reason, num_candidates)
      if (toponym != trueloc) {
        diff_surface.record_result(correct, reason, num_candidates)
        val (short, div) = Article.compute_short_form(trueloc)
        if (toponym != short)
          diff_short.record_result(correct, reason, num_candidates)
      }
    }

    def output_geotag_toponym_results() {
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
  //   document: The document (article, etc.) of the word.  Useful when a single
  //             file contains multiple such documents.
  //
  class GeogWord(val word: String) {
    var is_stop = false
    var is_toponym = false
    var coord: Coord = null
    var location: String = null
    var context: Array[(Int, String)] = null
    var document: String = null
  }

  abstract class GeotagToponymStrategy {
    def need_context(): Boolean
    def compute_score(geogword: GeogWord, art: TopoArticle): Double
  }

  // Find each toponym explicitly mentioned as such and disambiguate it
  // (find the correct geographic location) using the "link baseline", i.e.
  // use the location with the highest number of incoming links.
  class BaselineGeotagToponymStrategy(
    val baseline_strategy: String) extends GeotagToponymStrategy {
    def need_context() = false

    def compute_score(geogword: GeogWord, art: TopoArticle) = {
      if (baseline_strategy == "internal-link") {
        if (Opts.context_type == "region")
          art.find_regworddist().incoming_links
        else
          art.adjusted_incoming_links
      } else if (baseline_strategy == "num-articles") {
        if (Opts.context_type == "region")
          art.find_regworddist().num_arts_for_links
        else {
          val location = art.location
          location match {
            case x @ Division(_) => x.locs.length
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
    val use_baseline: Boolean) extends GeotagToponymStrategy {
    def need_context() = true

    def compute_score(geogword: GeogWord, art: TopoArticle) = {
      // FIXME FIXME!!! We are assuming that the baseline is "internal-link",
      // regardless of its actual settings.
      val thislinks = Article.log_adjust_incoming_links(
        art.adjusted_incoming_links)

      var distobj =
        if (Opts.context_type == "article") art.dist
        else art.find_regworddist()
      var totalprob = 0.0
      var total_word_weight = 0.0
      val (word_weight, baseline_weight) =
        if (!use_baseline) (1.0, 0.0)
        else if (Opts.naive_bayes_weighting == "equal") (1.0, 1.0)
        else (1 - Opts.naive_bayes_baseline_weight,
              Opts.naive_bayes_baseline_weight)
      for ((dist, word) <- geogword.context) {
        val lword =
          if (Opts.preserve_case_words) word else word.toLowerCase
        val wordprob = distobj.lookup_word(WordDist.memoize_word(lword))

        // Compute weight for each word, based on distance from toponym
        val thisweight =
          if (Opts.naive_bayes_weighting == "equal" ||
            Opts.naive_bayes_weighting == "equal-words") 1.0
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

  abstract class GeotagToponymEvaluator(
    strategy: GeotagToponymStrategy,
    stratname: String) extends TestFileEvaluator(stratname) {
    val results = new GeotagToponymResults()

    type Document = Iterable[GeogWord]

    // Given an evaluation file, read in the words specified, including the
    // toponyms.  Mark each word with the "document" (e.g. article) that it's
    // within.
    def iter_geogwords(filename: String): Iterable[GeogWord]

    // Retrieve the words yielded by iter_geowords() and separate by "document"
    // (e.g. article); yield each "document" as a list of such GeogWord objects.
    // If compute_context, also generate the set of "context" words used for
    // disambiguation (some window, e.g. size 20, of words around each
    // toponym).
    def iter_documents(filename: String) = {
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

      for ((k, g) <- iter_geogwords(filename).groupBy(_.document)) yield {
        if (k != null)
          errprint("Processing document %s...", k)
        val results = (for (word <- g) yield return_word(word)).toArray

        // Now compute context for words
        val nbcl = Opts.naive_bayes_context_len
        if (strategy.need_context()) {
          // First determine whether each word is a stopword
          for (i <- 0 until results.length) {
            // FIXME: Check that we aren't accessing a list or something with
            // O(N) random access
            // If a word tagged as a toponym is homonymous with a stopword, it
            // still isn't a stopword.
            results(i).is_stop = (results(i).coord == null &&
              (Stopwords.stopwords contains results(i).word))
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
                  if (!(Stopwords.stopwords contains x.word))
                } yield (dist, x.word)).toArray
            }
          }
        }

        (for (word <- results if word.coord != null) yield word).toIterable
      }
    }

    // Disambiguate the toponym, specified in GEOGWORD.  Determine the possible
    // locations that the toponym can map to, and call COMPUTE_SCORE on each one
    // to determine a score.  The best score determines the location considered
    // "correct".  Locations without a matching Wikipedia article are skipped.
    // The location considered "correct" is compared with the actual correct
    // location specified in the toponym, and global variables corresponding to
    // the total number of toponyms processed and number correctly determined are
    // incremented.  Various debugging info is output if 'debug' is set.
    // COMPUTE_SCORE is passed two arguments: GEOGWORD and the location to
    // compute the score of.

    def disambiguate_toponym(geogword: GeogWord) {
      val toponym = geogword.word
      val coord = geogword.coord
      if (coord == null) return // If no ground-truth, skip it
      val articles = TopoArticleTable.table.construct_candidates(toponym)
      var bestscore = Double.MinValue
      var bestart: TopoArticle = null
      if (articles.length == 0) {
        if (debug("some"))
          errprint("Unable to find any possibilities for %s", toponym)
      } else {
        if (debug("some")) {
          errprint("Considering toponym %s, coordinates %s",
            toponym, coord)
          errprint("For toponym %s, %d possible articles",
            toponym, articles.length)
        }
        for (iart <- articles) {
          val art = iart.asInstanceOf[TopoArticle]
          if (debug("some"))
            errprint("Considering article %s", art)
          val thisscore = strategy.compute_score(geogword, art)
          if (thisscore > bestscore) {
            bestscore = thisscore
            bestart = art
          }
        }
      }
      val correct =
        if (bestart != null)
          bestart.matches_coord(coord)
        else
          false

      val num_candidates = articles.length

      val reason =
        if (correct) null
        else {
          if (num_candidates == 0)
            "incorrect_with_no_candidates"
          else {
            val good_arts =
              (for { iart <- articles
                     val art = iart.asInstanceOf[TopoArticle]
                     if art.matches_coord(coord)
                   }
               yield art)
            if (good_arts == null)
              "incorrect_with_no_correct_candidates"
            else if (good_arts.length > 1)
              "incorrect_with_multiple_correct_candidates"
            else {
              val goodart = good_arts(0)
              if (goodart.incoming_links == None)
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

      results.record_geotag_toponym_result(correct, toponym,
        geogword.location, reason, num_candidates)

      if (debug("some") && bestart != null) {
        errprint("Best article = %s, score = %s, dist = %s, correct %s",
          bestart, bestscore, bestart.distance_to_coord(coord), correct)
      }
    }

    def evaluate_document(doc: Iterable[GeogWord], doctag: String) = {
      for (geogword <- doc)
        disambiguate_toponym(geogword)
      true
    }

    def output_results(isfinal: Boolean = false) {
      results.output_geotag_toponym_results()
    }
  }

  class TRCoNLLGeotagToponymEvaluator(
    strategy: GeotagToponymStrategy,
    stratname: String) extends GeotagToponymEvaluator(strategy, stratname) {
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
    def iter_geogwords(filename: String) = {
      var in_loc = false
      var wordstruct: GeogWord = null
      val lines = openr(filename, errors = "replace")
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
              wordstruct.coord = Coord(lat.toDouble, long.toDouble)
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
      iter_1()
    }
  }

  class ArticleGeotagToponymEvaluator(
    strategy: GeotagToponymStrategy,
    stratname: String) extends GeotagToponymEvaluator(strategy, stratname) {
    def iter_geogwords(filename: String) = {
      var title: String = null
      val titlere = """Article title: (.*)$""".r
      val linkre = """Link: (.*)$""".r
      val lines = openr(filename, errors = "replace")
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
              val trueart = args(0)
              var linkword = trueart
              if (args.length > 1)
                linkword = args(1)
              val word = new GeogWord(linkword)
              word.is_toponym = true
              word.location = trueart
              word.document = title
              val art = TopoArticleTable.table.lookup_article(trueart)
              if (art != null)
                word.coord = art.coord
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
      iter_1()
    }
  }

  class Gazetteer {
    // For each toponym (name of location), value is a list of Locality
    // items, listing gazetteer locations and corresponding matching
    // Wikipedia articles.
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
    //    process_eval_file(fname, count_toponyms, compute_context=false,
    //                      only_toponyms=true)
    //  }
  }

  object Gazetteer {
    // The one and only currently existing gazetteer.
    // FIXME: Eventually this and other static objects should go elsewhere.
    var gazetteer: Gazetteer = null
  }

  class WorldGazetteer(filename: String) extends Gazetteer {

    // Find the Wikipedia article matching an entry in the gazetteer.
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
    // For localities, add them to the region-map that covers the earth if
    // ADD_TO_REGION_MAP is true.

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
      val loc = new Locality(name, Coord(lat.toInt / 100., long.toInt / 100.),
        typ = typ, altnames = if (altnames != null) ", ".r.split(altnames) else null)
      loc.div = Division.find_division_note_point(loc, Seq(div1, div2, div3))
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

      // We start out looking for articles whose distance is very close,
      // then widen until we reach Opts.max_dist_for_close_match.
      var maxdist = 5
      var artmatch: TopoArticle = null
      breakable {
        while (maxdist <= Opts.max_dist_for_close_match) {
          artmatch =
            TopoArticleTable.table.find_match_for_locality(loc, maxdist)
          if (artmatch != null) break
          maxdist *= 2
        }
      }

      if (artmatch == null) {
        if (debug("lots"))
          errprint("Unmatched name %s", loc.name)
        return
      }

      // Record the match.
      loc.artmatch = artmatch
      artmatch.location = loc
      if (debug("lots"))
        errprint("Matched location %s (coord %s) with article %s, dist=%s",
          (loc.name, loc.coord, artmatch,
            spheredist(loc.coord, artmatch.coord)))
    }

    // Read in the data from the World gazetteer in FILENAME and find the
    // Wikipedia article matching each entry in the gazetteer.  For localities,
    // add them to the region-map that covers the earth if ADD_TO_REGION_MAP is
    // true.
    protected def read_world_gazetteer_and_match(filename: String) {
      errprint("Matching gazetteer entries in %s...", filename)
      val status = new StatusMessage("gazetteer entry")

      // Match each entry in the gazetteer
      breakable {
        for (line <- openr(filename)) {
          if (debug("lots"))
            errprint("Processing line: %s", line)
          match_world_gazetteer_entry(line)
          if (status.item_processed(maxtime = Opts.max_time_per_stage))
            break
        }
      }

      Division.finish_all()
      errprint("Finished matching %s gazetteer entries.",
        status.num_processed())
      output_resource_usage()
    }

    // Upon creation, populate gazetteer from file
    read_world_gazetteer_and_match(filename)
  }
}

