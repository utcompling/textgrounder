package opennlp.textgrounder
package gridlocate

import math.log

import langmodel._
import util.debug._
import util.print._
import util.math.logn
import util.metering._
import util.textdb.Row
import learning._

object Featurizer extends WordAsIntMemoizer { }

/**
 * A ranker for ranking cells in a grid as possible matches for a given
 * document (aka "grid-locating a document").
 *
 * @tparam Co Type of document's identifying coordinate (e.g. a lat/long tuple,
 *   a year, etc.), which tends to determine the grid structure.
 * @param ranker_name Name of the ranker, for output purposes
 * @param grid Grid containing the cells over which this ranker operates
 */
abstract class GridRanker[Co](
  val ranker_name: String,
  val grid: Grid[Co]
) extends Ranker[GridDoc[Co], GridCell[Co]] {
  /**
   * For a given language model (describing a test document), return
   * an Iterable of tuples, each listing a particular cell on the Earth
   * and a score of some sort.  The cells given in `include` must be
   * included in the list.  Higher scores are better.  The results should
   * be in sorted order, with better cells earlier.
   */
  def return_ranked_cells(lang_model: LangModel,
      include: Iterable[GridCell[Co]]):
    Iterable[(GridCell[Co], Double)]

  def evaluate(item: GridDoc[Co], include: Iterable[GridCell[Co]]) =
    return_ranked_cells(item.grid_lm, include)
}

/**
 * Object encapsulating a GridLocate data instance to be used by the
 * classifier that underlies the ranker. This corresponds to a document
 * in the training corpus and serves as the main part of an RTI (rerank
 * training instance, see `PointwiseClassifyingRerankerTrainer`).
 */
case class GridRankerInst[Co](
  doc: GridDoc[Co],
  candidates: IndexedSeq[GridCell[Co]],
  fv: AggregateFeatureVector
) extends DataInstance {
  final def feature_vector = fv
}

/**
 * A factory for generating "candidate instances", i.e. feature vectors
 * describing the properties of one of the possible candidates (cells) to
 * be chosen by a reranker for a given query (i.e. document). Thus, a
 * candidate instance for a reranker is a query-candidate (i.e. document-cell)
 * pair, or rather a feature vector describing this pair. In general, the
 * features describe the compatibility between the query and the candidate,
 * i.e. in this case the compatibility between the language models
 * (language models) of a document and a cell.
 *
 * The factory is in the form of a function that will generate a feature
 * vector when passed appropriate arguments: a document, a cell, the score
 * of the cell as produced by the original ranker, and a boolean indicating
 * whether we are generating the feature vector for use in training a model
 * or in evaluating a model. (This matters e.g. in the handling of unseen
 * words, which will modify the global language model during training but
 * not evaluation.)
 */
trait CandidateInstFactory[Co] extends (
  (GridDoc[Co], GridCell[Co], Double, Boolean) => FeatureVector
) {
  val featvec_factory =
    new SparseFeatureVectorFactory[Gram](word => Featurizer.unmemoize(word))
  val scoreword = "$score"

  /**
   * Return an Iterable of feature-value pairs for a document-cell pair,
   * usually describing similarities between the document and cell's
   * language models. Meant to be supplied by subclasses.
   *
   * @param doc Document of document-cell pair.
   * @param cell Cell of document-cell pair.
   */
  def get_features(doc: GridDoc[Co], cell: GridCell[Co]
    ): Iterable[(String, Double)]

  val logarithmic_base = 2.0

  def bin_logarithmically(feat: String, value: Double) = {
    assert(value >= 0)
    // This should work even when value is 0, yielding -2147483648
    // (Int.MinValue)
    val log = logn(value, logarithmic_base).toInt
    ("%s$%s" format (feat, log), 1.0)
  }

  def include_and_bin_logarithmically(feat: String, value: Double) = {
    Seq(feat -> value, bin_logarithmically(feat, value))
  }

  val fractional_increment = 0.1

  def bin_fractionally(feat: String, value: Double) = {
    assert(value >= 0 && value <= 1)
    val incr = (value / fractional_increment).toInt
    ("%s$%s" format (feat, incr), 1.0)
  }

  def include_and_bin_fractionally(feat: String, value: Double) = {
    Seq(feat -> value, bin_fractionally(feat, value))
  }

  /**
   * Generate a feature vector from a query-candidate (document-cell) pair.
   *
   * @param document Document serving as a query item
   * @param cell Cell serving as a candidate to be ranked
   * @param score Score for this cell as produced by the initial ranker
   * @param is_training Whether we are training or evaluating a model
   *   (see above)
   */
  def apply(doc: GridDoc[Co], cell: GridCell[Co], score: Double,
    is_training: Boolean) = {
    val feats_with_score =
      (get_features(doc, cell) ++ Iterable(scoreword -> score)).map {
        case (word, value) =>
          (Featurizer.memoize(word), value)
      }
    featvec_factory.make_feature_vector(feats_with_score, is_training)
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * nothing but the score passed in.
 */
class TrivialCandidateInstFactory[Co] extends CandidateInstFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = Iterable()
}

/**
 * A factory that combines the features of a set of subsidiary factories.
 */
class CombiningCandidateInstFactory[Co](
  val subsidiary_facts: Iterable[CandidateInstFactory[Co]]
) extends CandidateInstFactory[Co] {
  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = {
    // For each subsidiary factory, retrieve its features, then add the
    // index of the factory to the feature's name to disambiguate, and
    // concatenate all features.
    if (debug("combined-features")) {
      errprint("Document: %s", doc)
      errprint("Cell: %s", cell)
    }
    subsidiary_facts.zipWithIndex flatMap { case (fact, index) =>
      fact.get_features(doc, cell) map { case (item, count) =>
        val featname =
          // FIXME! Use item_to_string to be more general.
          // FIXME! May not be necessary to memoize like this. I don't think we
          // need to unmemoize the feature names (except for debugging
          // purposes), so it's enough just to OR the index onto the top bits
          // of the word, which is already a low integer due to memoization
          // (or if we change the memoization strategy in a way that generates
          // spread-out integers, we can just XOR the index onto the top bits
          // or hash the two numbers together; occasional feature clashes
          // aren't a big deal).
          "%s$%s" format (item, index)
        if (debug("combined-features")) {
          errprint("%s = %s", featname, count)
        }
        (featname, count)
      }
    }
  }
}

/**
 * A factory for generating candidate instances for a document, generating
 * separate features for each word.
 */
abstract class UnigramCandidateInstFactory[Co] extends
    CandidateInstFactory[Co] {
  /**
   * Return an Iterable of features corresponding to the given doc-cell
   * (query-candidate) pair, provided that Unigram rerank distributions
   * are being used.
   *
   * @param doc Document of document-cell pair.
   * @param doclm Unigram rerank language model of document.
   * @param cell Cell of document-cell pair.
   * @param celllm Unigram rerank language model of cell.
   */
  def get_unigram_features(doc: GridDoc[Co], doclm: UnigramLangModel,
    cell: GridCell[Co], celllm: UnigramLangModel): Iterable[(String, Double)]

  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val doclm = Unigram.check_unigram_lang_model(doc.rerank_lm)
    val celllm =
      Unigram.check_unigram_lang_model(cell.rerank_lm)
    get_unigram_features(doc, doclm, cell, celllm)
  }
}

/**
 * A factory for generating candidate instances for a document, generating
 * non-word-by-word features.
 */
class MiscCandidateInstFactory[Co] extends
    CandidateInstFactory[Co] {
  protected def ib(feat: String, value: Double) =
    include_and_bin_logarithmically(feat, value)

  protected def types_in_common(doclm: LangModel, celllm: LangModel) = {
    val doctypes = doclm.model.iter_keys.toSeq
    val celltypes = celllm.model.iter_keys.toSeq
    (doctypes intersect celltypes).size
  }

  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val doclm = doc.rerank_lm
    val celllm = cell.rerank_lm
    Iterable(
      ib("$cell-numdocs", cell.num_docs),
      ib("$cell-numtypes", celllm.model.num_types),
      ib("$cell-numtokens", celllm.model.num_tokens),
      ib("$cell-salience", cell.salience),
      ib("$doc-numtypes", doclm.model.num_types),
      ib("$doc-numtokens", doclm.model.num_tokens),
      ib("$doc-salience", doc.salience.getOrElse(0.0)),
      ib("$numtypes-quotient", celllm.model.num_types/doclm.model.num_types),
      ib("$numtokens-quotient", celllm.model.num_tokens/doclm.model.num_tokens),
      ib("$salience-quotient", cell.salience/doc.salience.getOrElse(0.0)),
      ib("$numtypes-diff", celllm.model.num_types - doclm.model.num_types),
      ib("$numtokens-diff", celllm.model.num_tokens - doclm.model.num_tokens),
      ib("$salience-diff", cell.salience - doc.salience.getOrElse(0.0)),
      ib("$types-in-common", types_in_common(doclm, celllm)),
      ib("$kldiv", doclm.kl_divergence(celllm)),
      ib("$symmetric-kldiv", doclm.symmetric_kldiv(celllm)),
      ib("$cossim", doclm.cosine_similarity(celllm)),
      ib("$nb-logprob", doclm.model_logprob(celllm))
    ).flatten
  }
}

/**
 * A factory for generating candidate instances for a document, generating
 * separate features for each word.
 */
abstract class WordByWordCandidateInstFactory[Co] extends
    UnigramCandidateInstFactory[Co] {
  /** Optionally return a per-word feature whose count in the document is
    * `count`, with specified document and cell language models. The
    * return value is a tuple of suffix describing the particular feature
    * class being returned, and feature value. The suffix will be appended
    * to the word itself to form the feature name. */
  def get_word_feature(word: Gram, doccount: Double, doclm: UnigramLangModel,
    celllm: UnigramLangModel): Option[(String, Double)]

  def get_unigram_features(doc: GridDoc[Co], doclm: UnigramLangModel,
    cell: GridCell[Co], celllm: UnigramLangModel) = {
    for ((word, count) <- doclm.model.iter_items;
         (suff, featval) <- get_word_feature(word, count, doclm, celllm))
      yield ("%s$%s" format (doclm.item_to_string(word), suff), featval)
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * the presence of words in the document.
 *
 * @param feattype How to compute the value assigned to the words:
 *
 * - `unigram-binary`: always assign 1
 * - `unigram-doc-count`: use document word count
 * - `unigram-cell-count`: use cell word count
 * - `unigram-doc-prob`: use document word probability
 * - `unigram-cell-prob`: use cell word probability
 * - any of the above with `-binned` added, which bins logarithmically
 */
class WordCandidateInstFactory[Co](feattype: String) extends
    WordByWordCandidateInstFactory[Co] {
  def get_word_feature(word: Gram, doccount: Double, doclm: UnigramLangModel,
      celllm: UnigramLangModel) = {
    val binned = feattype.endsWith("-binned")
    val basetype = feattype.replace("-binned", "")
    val wordval = basetype match {
      case "unigram-binary" => 1
      case "unigram-doc-count" => doccount
      case "unigram-cell-count" => celllm.model.get_item(word)
      case "unigram-doc-prob" => doclm.item_prob(word)
      case "unigram-cell-prob" => celllm.item_prob(word)
    }
    if (binned)
      Some(bin_logarithmically(feattype, wordval))
    else
      Some((feattype, wordval))
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * the presence of matching words between document and cell.
 *
 * @param feattype How to compute the value assigned to the words that are
 *   shared:
 *
 * - `matching-unigram-binary`: always assign 1
 * - `matching-unigram-doc-count`: use document word count
 * - `matching-unigram-cell-count`: use cell word count
 * - `matching-unigram-doc-prob`: use document word probability
 * - `matching-unigram-cell-prob`: use cell word probability
 * - `matching-unigram-count-product`: use product of document and cell word count
 * - `matching-unigram-count-quotient`: use quotient of cell and document word count
 * - `matching-unigram-prob-product`: use product of document and cell word prob
 * - `matching-unigram-prob-quotient`: use quotient of cell and document word prob
 * - `matching-unigram-kl`: use KL-divergence component for document/cell probs
 * - any of the above with `-binned` added, which bins logarithmically
 */
class WordMatchingCandidateInstFactory[Co](feattype: String) extends
    WordByWordCandidateInstFactory[Co] {
  def get_word_feature(word: Gram, doccount: Double, doclm: UnigramLangModel,
      celllm: UnigramLangModel) = {
    val cellcount = celllm.model.get_item(word)
    if (cellcount == 0.0)
      None
    else {
      val binned = feattype.endsWith("-binned")
      val basetype = feattype.replace("-binned", "")
      val wordval = basetype match {
        case "matching-unigram-binary" => 1
        case "matching-unigram-doc-count" => doccount
        case "matching-unigram-cell-count" => cellcount
        case "matching-unigram-count-product" => doccount * cellcount
        case "matching-unigram-count-quotient" => cellcount / doccount
        case _ => {
          val docprob = doclm.item_prob(word)
          val cellprob = celllm.item_prob(word)
          basetype match {
            case "matching-unigram-doc-prob" =>
              docprob
            case "matching-unigram-cell-prob" =>
              cellprob
            case "matching-unigram-prob-product" =>
              docprob * cellprob
            case "matching-unigram-prob-quotient" =>
              cellprob / docprob
            case "matching-unigram-kl" =>
              docprob*(log(docprob/cellprob))
          }
        }
      }
      if (binned)
        Some(bin_logarithmically(feattype, wordval))
      else
        Some((feattype, wordval))
    }
  }
}

/**
 * A factory for generating candidate instances for a document, generating
 * separate features for each n-gram.
 */
abstract class NgramByNgramCandidateInstFactory[Co] extends
    CandidateInstFactory[Co] {
  /** Optionally return a per-ngram feature whose count in the document is
    * `doccount`, with specified document and cell language models. The
    * return value is a tuple of suffix describing the particular feature
    * class being returned, and feature value. The suffix will be appended
    * to the ngram itself to form the feature name. */
  def get_ngram_feature(word: Gram, doccount: Double, doclm: NgramLangModel,
    celllm: NgramLangModel): Option[(String, Double)]

  def get_features(doc: GridDoc[Co], cell: GridCell[Co]) = {
    val doclm = Ngram.check_ngram_lang_model(doc.rerank_lm)
    val celllm =
      Ngram.check_ngram_lang_model(cell.lang_model.rerank_lm)
    for ((ngram, count) <- doclm.model.iter_items;
         (suff, featval) <- get_ngram_feature(ngram, count, doclm, celllm))
      // Generate a feature name by concatenating the words. This may
      // conceivably lead to clashes if a word actually has the
      // separator in it, but that's unlikely and doesn't really matter
      // anyway.
      yield ("%s$%s" format (Ngram.unmemoize(ngram) mkString "|", suff),
        featval)
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * the presence of ngrams in the document.
 *
 * @param feattype How to compute the value assigned to the ngrams:
 *
 * - `ngram-binary`: always assign 1
 * - `ngram-doc-count`: use document ngram count
 * - `ngram-cell-count`: use cell ngram count
 * - `ngram-doc-prob`: use document ngram probability
 * - `ngram-cell-prob`: use cell ngram probability
 * - any of the above with `-binned` added, which bins logarithmically
 */
class NgramCandidateInstFactory[Co](feattype: String) extends
    NgramByNgramCandidateInstFactory[Co] {
  def get_ngram_feature(ngram: Gram, doccount: Double, doclm: NgramLangModel,
      celllm: NgramLangModel) = {
    val binned = feattype.endsWith("-binned")
    val basetype = feattype.replace("-binned", "")
    val ngramval = basetype match {
      case "ngram-binary" => 1
      case "ngram-doc-count" => doccount
      case "ngram-cell-count" => celllm.model.get_item(ngram)
      case "ngram-doc-prob" => doclm.item_prob(ngram)
      case "ngram-cell-prob" => celllm.item_prob(ngram)
    }
    if (binned)
      Some(bin_logarithmically(feattype, ngramval))
    else
      Some((feattype, ngramval))
  }
}

/**
 * A simple factory for generating candidate instances for a document, using
 * the presence of matching ngrams between document and cell.
 *
 * @param feattype How to compute the value assigned to the ngrams that are
 *   shared:
 *
 * - `matching-ngram-binary`: always assign 1
 * - `matching-ngram-doc-count`: use document ngram count
 * - `matching-ngram-cell-count`: use cell ngram count
 * - `matching-ngram-doc-prob`: use document ngram probability
 * - `matching-ngram-cell-prob`: use cell ngram probability
 * - `matching-ngram-count-product`: use product of document and cell ngram count
 * - `matching-ngram-count-quotient`: use quotient of cell and document ngram count
 * - `matching-ngram-prob-product`: use product of document and cell ngram prob
 * - `matching-ngram-prob-quotient`: use quotient of cell and document ngram prob
 * - `matching-ngram-kl`: use KL-divergence component for document/cell probs
 * - any of the above with `-binned` added, which bins logarithmically
 */
class NgramMatchingCandidateInstFactory[Co](feattype: String) extends
    NgramByNgramCandidateInstFactory[Co] {
  def get_ngram_feature(ngram: Gram, doccount: Double, doclm: NgramLangModel,
      celllm: NgramLangModel) = {
    val cellcount = celllm.model.get_item(ngram)
    if (cellcount == 0.0)
      None
    else {
      val binned = feattype.endsWith("-binned")
      val basetype = feattype.replace("-binned", "")
      val ngramval = basetype match {
        case "matching-ngram-binary" => 1
        case "matching-ngram-doc-count" => doccount
        case "matching-ngram-cell-count" => cellcount
        case "matching-ngram-count-product" => doccount * cellcount
        case "matching-ngram-count-quotient" => cellcount / doccount
        case _ => {
          val docprob = doclm.item_prob(ngram)
          val cellprob = celllm.item_prob(ngram)
          basetype match {
            case "matching-ngram-doc-prob" =>
              docprob
            case "matching-ngram-cell-prob" =>
              cellprob
            case "matching-ngram-prob-product" =>
              docprob * cellprob
            case "matching-ngram-prob-quotient" =>
              cellprob / docprob
            case "matching-ngram-kl" =>
              docprob*(log(docprob/cellprob))
          }
        }
      }
      if (binned)
        Some(bin_logarithmically(feattype, ngramval))
      else
        Some((feattype, ngramval))
    }
  }
}

/**
 * A grid reranker, i.e. a pointwise reranker for doing reranking in a
 * GridLocate context, based on a grid ranker (for ranking cells in a grid
 * as possible matches for a given document).
 * See `PointwiseClassifyingReranker`.
 */
abstract class PointwiseGridReranker[Co](ranker_name: String,
  grid: Grid[Co]
) extends GridRanker[Co](ranker_name, grid)
  with PointwiseClassifyingReranker[GridDoc[Co], GridCell[Co]] {
    def return_ranked_cells(lang_model: LangModel,
        include: Iterable[GridCell[Co]]) =
      initial_ranker.asInstanceOf[GridRanker[Co]].return_ranked_cells(
        lang_model, include)
}

/**
 * A grid reranker using a linear classifier.  See `PointwiseGridReranker`.
 *
 * @param trainer Factory object for training a linear classifier used for
 *   pointwise reranking.
 */
abstract class LinearClassifierGridRerankerTrainer[Co](
  val trainer: SingleWeightLinearClassifierTrainer[GridRankerInst[Co]]
) extends PointwiseClassifyingRerankerTrainer[
    GridDoc[Co], GridCell[Co], DocStatus[Row], GridRankerInst[Co]
    ] { self =>
  protected def create_rerank_classifier(
    data: Iterable[(GridRankerInst[Co], Int)]
  ) = {
    errprint("Training linear classifier ...")
    errprint("Number of training items: %s", data.size)
    val num_total_feats =
      data.map(_._1.feature_vector.length.toLong).sum
    val num_total_stored_feats =
      data.map(_._1.feature_vector.stored_entries.toLong).sum
    errprint("Total number of features in all training items: %s",
      num_total_feats)
    errprint("Avg number of features per training item: %.2f",
      num_total_feats.toDouble / data.size)
    errprint("Total number of stored features in all training items: %s",
      num_total_stored_feats)
    errprint("Avg number of stored features per training item: %.2f",
      num_total_stored_feats.toDouble / data.size)
    trainer(data)
  }

  /**
   * Actually create a reranker object, given a rerank classifier and
   * initial ranker.
   */
  override protected def create_reranker(
    _rerank_classifier: ScoringClassifier,
    _initial_ranker: Ranker[GridDoc[Co], GridCell[Co]]
  ) = {
    val grid_ir = _initial_ranker.asInstanceOf[GridRanker[Co]]
    new PointwiseGridReranker[Co](grid_ir.ranker_name, grid_ir.grid) {
      protected val rerank_classifier = _rerank_classifier
      protected val initial_ranker = _initial_ranker
      val top_n = self.top_n
      protected def create_candidate_evaluation_instance(query: GridDoc[Co],
          candidate: GridCell[Co], initial_score: Double) = {
        self.create_candidate_evaluation_instance(query, candidate,
          initial_score)
      }
    }
  }

  /**
   * Train a reranker, based on external training data.
   */
  override def apply(training_data: Iterable[DocStatus[Row]]) =
    super.apply(training_data).
      asInstanceOf[PointwiseGridReranker[Co]]

  override def format_query_item(item: GridDoc[Co]) = {
    "%s, lm=%s" format (item, item.rerank_lm.debug_string)
  }
}
