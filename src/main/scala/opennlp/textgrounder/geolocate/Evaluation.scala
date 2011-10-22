package opennlp.textgrounder.geolocate

import NlpUtil._
import Distances._
import Debug._
import GeolocateDriver.Opts

import math._
import collection.mutable
import util.control.Breaks._
import java.io._

/////////////////////////////////////////////////////////////////////////////
//                 General statistics on evaluation results                //
/////////////////////////////////////////////////////////////////////////////

// incorrect_reasons is a map from ID's for reasons to strings describing
// them.
class EvalStats(incorrect_reasons: Map[String, String]) {
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

class EvalStatsWithRank(
  max_rank_for_credit: Int = 10
) extends EvalStats(Map[String, String]()) {
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

//////// Statistics for geotagging documents/articles

class GeotagDocumentEvalStats(
  max_rank_for_credit: Int = 10
) extends EvalStatsWithRank(max_rank_for_credit) {
  // "True dist" means actual distance in km's or whatever.
  // "Degree dist" is the distance in degrees.
  val true_dists = mutable.Buffer[Double]()
  val degree_dists = mutable.Buffer[Double]()
  val oracle_true_dists = mutable.Buffer[Double]()
  val oracle_degree_dists = mutable.Buffer[Double]()

  def record_result(rank: Int, pred_true_dist: Double,
      pred_degree_dist: Double) {
    super.record_result(rank)
    true_dists += pred_true_dist
    degree_dists += pred_degree_dist
  }

  def record_oracle_result(oracle_true_dist: Double,
      oracle_degree_dist: Double) {
    oracle_true_dists += oracle_true_dist
    oracle_degree_dists += oracle_degree_dist
  }

  override def output_incorrect_results() {
    super.output_incorrect_results()
    def miles_and_km(miledist: Double) = {
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
    errprint("  Mean oracle true error distance = %s",
      miles_and_km(mean(oracle_true_dists)))
    errprint("  Median oracle true error distance = %s",
      miles_and_km(median(oracle_true_dists)))
  }
}

/**
 * Class for statistics for geotagging documents/articles, with separate
 * sets of statistics for different intervals of error distances and
 * number of articles in true region.
 */

class GroupedGeotagDocumentEvalStats {

  def create_doc() = new GeotagDocumentEvalStats()
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
  def docmap() = defaultmap[Double, GeotagDocumentEvalStats](create_doc())
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

  def record_result(res: ArticleEvaluationResult) {
    all_document.record_result(res.rank, res.pred_truedist, res.pred_degdist)
    val naitr = docs_by_naitr.get_collector(res.num_arts_in_true_region)
    naitr.record_result(res.rank, res.pred_truedist, res.pred_degdist)

    val fracinc = dist_fraction_increment
    val rounded_true_truedist = fracinc * floor(res.true_truedist / fracinc)
    val rounded_true_degdist = fracinc * floor(res.true_degdist / fracinc)

    all_document.record_oracle_result(res.true_truedist, res.true_degdist)
    docs_by_true_dist_to_true_center(rounded_true_truedist).
      record_result(res.rank, res.pred_truedist, res.pred_degdist)
    docs_by_degree_dist_to_true_center(rounded_true_degdist).
      record_result(res.rank, res.pred_truedist, res.pred_degdist)

    docs_by_true_dist_to_pred_center.get_collector(res.pred_truedist).
      record_result(res.rank, res.pred_truedist, res.pred_degdist)
    docs_by_degree_dist_to_pred_center.get_collector(res.pred_degdist).
      record_result(res.rank, res.pred_truedist, res.pred_degdist)
  }

  def record_other_stat(othertype: String) {
    all_document.record_other_stat(othertype)
  }

  def output_results(all_results: Boolean = false) {
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
        val lowrange = truedist * miles_per_region
        val highrange = ((truedist + dist_fraction_increment) *
          miles_per_region)
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

trait EvaluationDocument {
}

trait EvaluationResult {
}

/**
  Abstract class for reading documents from a test file and evaluating on them.
 */
abstract class TestFileEvaluator(stratname: String) {
  var documents_processed = 0

  type Document <: EvaluationDocument

  /**
    Return an Iterable listing the documents retrievable from the given
    filename.
   */
  def iter_documents(filename: String): Iterable[Document]

  /**
    Return true if document would be skipped; false if processed and
    evaluated.
   */
  def would_skip_document(doc: Document, doctag: String) = false

  /**
    Return true if document was actually processed and evaluated; false
    if skipped.
   */
  def evaluate_document(doc: Document, doctag: String):
    EvaluationResult

  /**
    Output results so far.  If 'isfinal', this is the last call, so
    output more results.
   */
  def output_results(isfinal: Boolean = false): Unit
}

abstract class EvaluationOutputter {
  def evaluate_and_output_results(files: Iterable[String]): Unit
}

class DefaultEvaluationOutputter(stratname: String, evalobj: TestFileEvaluator
    ) extends EvaluationOutputter {
  val results = mutable.Map[EvaluationDocument, EvaluationResult]()
  /**
    Evaluate on all of the given files, outputting periodic results and
    results after all files are done.  If the evaluator uses articles as
    documents (so that it doesn't need any external test files), the value
    of 'files' should be a sequence of one item, which is null. (If an
    empty sequence is passed in, no evaluation will happen.)

    Also returns an object containing the results.
   */
  def evaluate_and_output_results(files: Iterable[String]) {
    val task = new MeteredTask("document", "evaluating")
    var last_elapsed = 0.0
    var last_processed = 0
    var skip_initial = Opts.skip_initial_test_docs
    var skip_n = 0

    class EvaluationFileProcessor extends FileProcessor {
      override def begin_process_directory(dir: File) {
        errprint("Processing evaluation directory %s...", dir)
      }

      /* Process all documents in a given file.  If return value is false,
         processing was interrupted due to a limit being reached, and
         no more files should be processed. */
      def process_file(filename: String): Boolean = {
        if (filename != null)
          errprint("Processing evaluation file %s...", filename)
        for (doc <- evalobj.iter_documents(filename)) {
          // errprint("Processing document: %s", doc)
          val num_processed = task.num_processed
          val doctag = "#%d" format (1 + num_processed)
          if (evalobj.would_skip_document(doc, doctag))
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
              val result = evalobj.evaluate_document(doc, doctag)
              assert(result != null)
              results(doc) = result
            }
            task.item_processed()
            val new_elapsed = task.elapsed_time
            val new_processed = task.num_processed

            // If max # of docs reached, stop
            if ((Opts.num_test_docs > 0 &&
              new_processed >= Opts.num_test_docs)) {
              errprint("")
              errprint("Stopping because limit of %s documents reached",
                Opts.num_test_docs)
              task.finish()
              return false
            }

            // If five minutes and ten documents have gone by, print out results
            if ((new_elapsed - last_elapsed >= 300 &&
              new_processed - last_processed >= 10)) {
              errprint("Results after %d documents (strategy %s):",
                task.num_processed, stratname)
              evalobj.output_results(isfinal = false)
              errprint("End of results after %d documents (strategy %s):",
                task.num_processed, stratname)
              last_elapsed = new_elapsed
              last_processed = new_processed
            }
          }
        }

        return true
      }
    }

    new EvaluationFileProcessor().process_files(files)

    task.finish()

    errprint("")
    errprint("Final results for strategy %s: All %d documents processed:",
      stratname, task.num_processed)
    errprint("Ending operation at %s", curtimehuman())
    evalobj.output_results(isfinal = true)
    errprint("Ending final results for strategy %s", stratname)
  }
}

abstract class GeotagDocumentEvaluator(
  strategy: GeotagDocumentStrategy,
  stratname: String
) extends TestFileEvaluator(stratname) {
  val evalstats = new GroupedGeotagDocumentEvalStats()

  def output_results(isfinal: Boolean = false) {
    evalstats.output_results(all_results = isfinal)
  }
}

case class ArticleEvaluationResult(
  article: StatArticle,
  rank: Int,
  pred_latind: Regind,
  pred_longind: Regind) extends EvaluationResult {

  val true_statreg = StatRegion.find_region_for_coord(article.coord)
  val num_arts_in_true_region = true_statreg.worddist.num_arts_for_word_dist
  val (true_latind, true_longind) = coord_to_stat_region_indices(article.coord)
  val true_center = stat_region_indices_to_center_coord(true_latind, true_longind)
  val true_truedist = spheredist(article.coord, true_center)
  val true_degdist = degree_dist(article.coord, true_center)
  val pred_center =
    stat_region_indices_to_center_coord(pred_latind, pred_longind)
  val pred_truedist = spheredist(article.coord, pred_center)
  val pred_degdist = degree_dist(article.coord, pred_center)
}

/**
  Class to do document geotagging on articles from the article data, in
  the dev or test set.
 */
class ArticleGeotagDocumentEvaluator(
  strategy: GeotagDocumentStrategy,
  stratname: String
) extends GeotagDocumentEvaluator(strategy, stratname) {

  type Document = StatArticle
  type DocumentResult = ArticleEvaluationResult

  // Debug flags:
  //
  //  gridrank: For the given test article number (starting at 1), output
  //            a grid of the predicted rank for regions around the true
  //            region.  Multiple articles can have the rank output, e.g.
  //
  //            --debug 'gridrank=45,58'
  //
  //            (This will output info for articles 45 and 58.)
  //
  //  gridranksize: Size of the grid, in numbers of articles on a side.
  //                This is a single number, and the grid will be a square
  //                centered on the true region.
  register_list_debug_param("gridrank")
  debugval("gridranksize") = "11"

  def iter_documents(filename: String) = {
    assert(filename == null)
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
      if (Opts.max_time_per_stage == 0.0 && Opts.num_training_docs == 0)
        warning("Can't evaluate article %s without distribution", article)
      evalstats.record_other_stat("Skipped articles")
      true
    } else false
  }

  def evaluate_document(article: StatArticle, doctag: String):
      EvaluationResult = {
    if (would_skip_document(article, doctag))
      return null
    assert(article.dist.finished)
    val (true_latind, true_longind) =
      coord_to_stat_region_indices(article.coord)
    if (debug("lots") || debug("commontop")) {
      val true_statreg = StatRegion.find_region_for_coord(article.coord)
      val naitr = true_statreg.worddist.num_arts_for_word_dist
      errprint(
        "Evaluating article %s with %s word-dist articles in true region",
        article, naitr)
    }

    /* That is:

       pred_regs = List of predicted regions, from best to worst
       true_rank = Rank of true region among predicted regions
       pred_latind, pred_longind = Indices of topmost predicted region
     */
    val (pred_regs, true_rank, pred_latind, pred_longind) = {
      if (Opts.oracle_results)
        (null, 1, true_latind, true_longind)
      else {
        def get_computed_results() = {
          val regs = strategy.return_ranked_regions(article.dist).toArray
          var rank = 1
          var broken = false
          breakable {
            for ((reg, value) <- regs) {
              if (reg.latind.get == true_latind &&
                  reg.longind.get == true_longind) {
                broken = true
                break
              }
              rank += 1
            }
          }
          if (!broken)
            rank = 1000000000
          (regs, rank, regs(0)._1.latind.get, regs(0)._1.longind.get)
        }

        get_computed_results()
      }
    }
    val result =
      new ArticleEvaluationResult(article, true_rank, pred_latind, pred_longind)

    val want_indiv_results =
      !Opts.oracle_results && !Opts.no_individual_results
    evalstats.record_result(result)
    if (result.num_arts_in_true_region == 0) {
      evalstats.record_other_stat(
        "Articles with no training articles in region")
    }
    if (want_indiv_results) {
      errprint("%s:Article %s:", doctag, article)
      errprint("%s:  %d types, %d tokens",
        doctag, article.dist.counts.size, article.dist.total_tokens)
      errprint("%s:  true region at rank: %s", doctag, true_rank)
      errprint("%s:  true region: %s", doctag, result.true_statreg)
      for (i <- 0 until 5) {
        errprint("%s:  Predicted region (at rank %s): %s",
          doctag, i + 1, pred_regs(i)._1)
      }
      errprint("%s:  Distance %.2f miles to true region center at %s",
        doctag, result.true_truedist, result.true_center)
      errprint("%s:  Distance %.2f miles to predicted region center at %s",
        doctag, result.pred_truedist, result.pred_center)
      assert(doctag(0) == '#')
      if (debug("gridrank") ||
        (debuglist("gridrank") contains doctag.drop(1))) {
        val grsize = debugval("gridranksize").toInt
        val min_latind = true_latind - grsize / 2
        val max_latind = min_latind + grsize - 1
        val min_longind = true_longind - grsize / 2
        val max_longind = min_longind + grsize - 1
        val grid = mutable.Map[(Regind, Regind), (StatRegion, Double, Int)]()
        for (((reg, value), rank) <- pred_regs zip (1 to pred_regs.length)) {
          val (la, lo) = (reg.latind.get, reg.longind.get)
          if (la >= min_latind && la <= max_latind &&
            lo >= min_longind && lo <= max_longind)
            grid((la, lo)) = (reg, value, rank)
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
                val (reg, value, rank) = regvalrank
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

    return result
  }
}

class TitledDocumentResult extends EvaluationResult {
}

class PCLTravelGeotagDocumentEvaluator(
  strategy: GeotagDocumentStrategy,
  stratname: String
) extends GeotagDocumentEvaluator(strategy, stratname) {
  case class TitledDocument(
    title: String, text: String) extends EvaluationDocument 
  type Document = TitledDocument
  type DocumentResult = TitledDocumentResult

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
    val dist = WordDist()
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
      if (debug("pcl-travel")) {
        errprint("  Rank %d, goodness %g:", rank, vall)
        errprint(reg.struct().toString) // indent=4
      } else
        errprint("  Rank %d, goodness %g: %s", rank, vall, reg.shortstr())
    }

    new TitledDocumentResult()
  }
}
