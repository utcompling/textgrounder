///////////////////////////////////////////////////////////////////////////////
//  DiscountedUnigramWordDist.scala
//
//  Copyright (C) 2010, 2011, 2012 Ben Wing, The University of Texas at Austin
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

package opennlp.textgrounder.worddist

import math._

import opennlp.textgrounder.util.collectionutil._
import opennlp.textgrounder.util.printutil.errprint

import opennlp.textgrounder.gridlocate.GridLocateDriver.Debug._
// FIXME! For --tf-idf
import opennlp.textgrounder.gridlocate.GridLocateDriver
import opennlp.textgrounder.gridlocate.GenericTypes._

import WordDist.memoizer._

abstract class DiscountedUnigramWordDistFactory(
    val interpolate: Boolean
  ) extends UnigramWordDistFactory {
  // Estimate of number of unseen word types for all documents
  var total_num_unseen_word_types = 0

  /**
   * Overall probabilities over all documents of seeing a word in a document,
   * for all words seen at least once in any document, computed using the
   * empirical frequency of a word among all documents, adjusted by the mass
   * to be assigned to globally unseen words (words never seen at all), i.e.
   * the value in 'globally_unseen_word_prob'.  We start out by storing raw
   * counts, then adjusting them.
   */
  var overall_word_probs = create_word_double_map()
  var owp_adjusted = false
  var document_freq = create_word_double_map()
  var num_documents = 0
  var global_normalization_factor = 0.0

  override def note_dist_globally(dist: WordDist) {
    val udist = dist.asInstanceOf[DiscountedUnigramWordDist]
    super.note_dist_globally(dist)
    if (dist.note_globally) {
      assert(!owp_adjusted)
      for ((word, count) <- udist.model.iter_items) {
        if (!(overall_word_probs contains word))
          total_num_word_types += 1
        // Record in overall_word_probs; note more tokens seen.
        overall_word_probs(word) += count
        // Our training docs should never have partial (interpolated) counts.
        assert (count == count.toInt)
        total_num_word_tokens += count.toInt
        // Note document frequency of word
        document_freq(word) += 1
      }
      num_documents += 1
    }
    if (debug("lots")) {
      errprint("""For word dist, total tokens = %s, unseen_mass = %s, overall unseen mass = %s""",
        udist.model.num_tokens, udist.unseen_mass, udist.overall_unseen_mass)
    }
  }

  // The total probability mass to be assigned to words not seen at all in
  // any document, estimated using Good-Turing smoothing as the unadjusted
  // empirical probability of having seen a word once.  No longer used at
  // all in the "new way".
  // var globally_unseen_word_prob = 0.0

  // For documents whose word counts are not known, use an empty list to
  // look up in.
  // unknown_document_counts = ([], [])

  def finish_global_distribution() {
    /* We do in-place conversion of counts to probabilities.  Make sure
       this isn't done twice!! */
    assert (!owp_adjusted)
    owp_adjusted = true
    // A holdout from the "old way".
    val globally_unseen_word_prob = 0.0
    if (GridLocateDriver.Params.tf_idf) {
      for ((word, count) <- overall_word_probs)
        overall_word_probs(word) =
          count*math.log(num_documents/document_freq(word))
    }
    global_normalization_factor = ((overall_word_probs.values) sum)
    for ((word, count) <- overall_word_probs)
      overall_word_probs(word) = (
        count.toDouble/global_normalization_factor*(1.0 - globally_unseen_word_prob))
  }
}

abstract class DiscountedUnigramWordDist(
  gen_factory: WordDistFactory,
  note_globally: Boolean
) extends UnigramWordDist(gen_factory, note_globally) {
  type TThis = DiscountedUnigramWordDist
  type TKLCache = DiscountedUnigramKLDivergenceCache
  def dufactory = gen_factory.asInstanceOf[DiscountedUnigramWordDistFactory]

  /** Total probability mass to be assigned to all words not
      seen in the document.  This indicates how much mass to "discount" from
      the unsmoothed (maximum-likelihood) estimated language model of the
      document.  This can be document-specific and is one of the two basic
      differences between the smoothing methods investigated here:
      
      1. Jelinek-Mercer uses a constant value.
      2. Dirichlet uses a value that is related to document length, getting
         smaller as document length increases.
      3. Pseudo Good-Turing, motivated by Good-Turing smoothing, computes this
         mass as the unadjusted empirical probability of having seen a word
         once.

      The other difference is whether to do interpolation or back-off.
      This only affects words that exist in the unsmoothed model.  With
      interpolation, we mix the unsmoothed and global models, using the
      discount value.  With back-off, we only use the unsmoothed model in
      such a case, using the global model only for words unseen in the
      unsmoothed model.

      In other words:

      1. With interpolation, we compute the probability as
      
          COUNTS[W]/TOTAL_TOKENS*(1 - UNSEEN_MASS) + 
            UNSEEN_MASS * overall_word_probs[W]

         For unseen words, only the second term is non-zero.

      2. With back-off, for words with non-zero MLE counts, we compute
         the probability as

          COUNTS[W]/TOTAL_TOKENS*(1 - UNSEEN_MASS)

         For other words, we compute the probability as

        UNSEEN_MASS * (overall_word_probs[W] / OVERALL_UNSEEN_MASS)

        The idea is that overall_word_probs[W] / OVERALL_UNSEEN_MASS is
        an estimate of p(W | W not in A).  We have to divide by
        OVERALL_UNSEEN_MASS to make these probabilities be normalized
        properly.  We scale p(W | W not in A) by the total probability mass
        we have available for all words not seen in A.

      3. An additional possibility is that we are asked the probability of
         a word never seen at all.  The old code I wrote tried to assign
         a non-zero probability to such words using the formula

        UNSEEN_MASS * globally_unseen_word_prob / NUM_UNSEEN_WORDS

        where NUM_UNSEEN_WORDS is an estimate of the total number of words
        "exist" but haven't been seen in any documents.  Based on Good-Turing
        motivation, we used the number of words seen once in any document.
        This certainly underestimates this number if not too many documents
        have been seen but might be OK if many documents seen.

        The paper on this subject suggests assigning zero probability to
        such words and ignoring them entirely in calculations if/when they
        occur in a query.
   */
  var unseen_mass = 0.5
  /**
     Probability mass assigned in 'overall_word_probs' to all words not seen
     in the document.  This is 1 - (sum over W in A of overall_word_probs[W]).
     See above.
   */
  var overall_unseen_mass = 1.0

  def innerToString = ", %.2f unseen mass" format unseen_mass

  var normalization_factor = 0.0

  /**
   * Here we compute the value of `overall_unseen_mass`, which depends
   * on the global `overall_word_probs` computed from all of the
   * distributions.
   */
  protected def imp_finish_after_global() {
    val factory = dufactory

    // Make sure that overall_word_probs has been computed properly.
    assert(factory.owp_adjusted)

    if (factory.interpolate)
      overall_unseen_mass = 1.0
    else
      overall_unseen_mass = 1.0 - (
        // NOTE NOTE NOTE! The toSeq needs to be added for some reason; if not,
        // the computation yields different values, which cause a huge loss of
        // accuracy (on the order of 10-15%).  I have no idea why; I suspect a
        // Scala bug. (SCALABUG) (Or, it used to occur when the code read
        // 'counts.keys.toSeq'; who knows now.)
        (for (ind <- model.iter_keys.toSeq)
          yield factory.overall_word_probs(ind)) sum)
    if (GridLocateDriver.Params.tf_idf) {
      for ((word, count) <- model.iter_items)
        model.set_item(word,
          count*log(factory.num_documents/factory.document_freq(word)))
    }
    normalization_factor = model.num_tokens
    //if (use_sorted_list)
    //  counts = new SortedList(counts)
    if (debug("discount-factor") || debug("discountfactor"))
      errprint("For distribution %s, norm_factor = %g, model.num_tokens = %s, unseen_mass = %g"
        format (this, normalization_factor, model.num_tokens, unseen_mass))
  }

  def fast_kl_divergence(cache: KLDivergenceCache, other: WordDist,
      partial: Boolean = false) = {
    FastDiscountedUnigramWordDist.fast_kl_divergence(
      this.asInstanceOf[TThis], cache.asInstanceOf[TKLCache],
      other.asInstanceOf[TThis], interpolate = dufactory.interpolate,
      partial = partial)
  }

  def cosine_similarity(other: WordDist, partial: Boolean = false,
      smoothed: Boolean = false) = {
    if (smoothed)
      FastDiscountedUnigramWordDist.fast_smoothed_cosine_similarity(
        this.asInstanceOf[TThis], other.asInstanceOf[TThis],
        partial = partial)
    else
      FastDiscountedUnigramWordDist.fast_cosine_similarity(
        this.asInstanceOf[TThis], other.asInstanceOf[TThis],
        partial = partial)
  }

  def kl_divergence_34(other: UnigramWordDist) = {
    val factory = dufactory
    var overall_probs_diff_words = 0.0
    for (word <- other.model.iter_keys if !(model contains word)) {
      overall_probs_diff_words += factory.overall_word_probs(word)
    }

    inner_kl_divergence_34(other.asInstanceOf[TThis],
      overall_probs_diff_words)
  }
      
  /**
   * Actual implementation of steps 3 and 4 of KL-divergence computation, given
   * a value that we may want to compute as part of step 2.
   */
  def inner_kl_divergence_34(other: TThis,
      overall_probs_diff_words: Double) = {
    var kldiv = 0.0

    // 3. For words seen in neither dist but seen globally:
    // You can show that this is
    //
    // factor1 = (log(self.unseen_mass) - log(self.overall_unseen_mass)) -
    //           (log(other.unseen_mass) - log(other.overall_unseen_mass))
    // factor2 = self.unseen_mass / self.overall_unseen_mass * factor1
    // kldiv = factor2 * (sum(words seen globally but not in either dist)
    //                    of overall_word_probs[word]) 
    //
    // The final sum
    //   = 1 - sum(words in self) overall_word_probs[word]
    //       - sum(words in other, not self) overall_word_probs[word]
    //   = self.overall_unseen_mass
    //       - sum(words in other, not self) overall_word_probs[word]
    //
    // So we just need the sum over the words in other, not self.
    //
    // Note that the above formula was derived using back-off, but it
    // still applies in interpolation.  For words seen in neither dist,
    // the only difference between back-off and interpolation is that
    // the "overall_unseen_mass" factors for all distributions are
    // effectively 1.0 (and the corresponding log terms above disappear).

    val factor1 = ((log(unseen_mass) - log(overall_unseen_mass)) -
               (log(other.unseen_mass) - log(other.overall_unseen_mass)))
    val factor2 = unseen_mass / overall_unseen_mass * factor1
    val the_sum = overall_unseen_mass - overall_probs_diff_words
    kldiv += factor2 * the_sum

    // 4. For words never seen at all:
    /* The new way ignores these words entirely.
    val p = (unseen_mass*factory.globally_unseen_word_prob /
          factory.total_num_unseen_word_types)
    val q = (other.unseen_mass*factory.globally_unseen_word_prob /
          factory.total_num_unseen_word_types)
    kldiv += factory.total_num_unseen_word_types*(p*(log(p) - log(q)))
    */
    kldiv
  }

  def lookup_word(word: Word) = {
    val factory = dufactory
    assert(finished)
    if (factory.interpolate) {
      val wordcount = if (model contains word) model.get_item(word) else 0.0
      // if (debug("some")) {
      //   errprint("Found counts for document %s, num word types = %s",
      //            doc, wordcounts(0).length)
      //   errprint("Unknown prob = %s, overall_unseen_mass = %s",
      //            unseen_mass, overall_unseen_mass)
      // }
      val owprob = factory.overall_word_probs.getOrElse(word, 0.0)
      val mle_wordprob = wordcount.toDouble/normalization_factor
      val wordprob = mle_wordprob*(1.0 - unseen_mass) + owprob*unseen_mass
      if (debug("lots"))
        errprint("Word %s, seen in document, wordprob = %s",
                 unmemoize_string(word), wordprob)
      wordprob
    } else {
      val retval =
        if (!(model contains word)) {
          factory.overall_word_probs.get(word) match {
            case None => {
              /*
              The old way:

              val wordprob = (unseen_mass*factory.globally_unseen_word_prob
                        / factory.total_num_unseen_word_types)
              */
              /* The new way: Just return 0 */
              val wordprob = 0.0
              if (debug("lots"))
                errprint("Word %s, never seen at all, wordprob = %s",
                         unmemoize_string(word), wordprob)
              wordprob
            }
            case Some(owprob) => {
              val wordprob = unseen_mass * owprob / overall_unseen_mass
              //if (wordprob <= 0)
              //  warning("Bad values; unseen_mass = %s, overall_word_probs[word] = %s, overall_unseen_mass = %s",
              //    unseen_mass, factory.overall_word_probs[word],
              //    factory.overall_unseen_mass)
              if (debug("lots"))
                errprint("Word %s, seen but not in document, wordprob = %s",
                         unmemoize_string(word), wordprob)
              wordprob
            }
          }
        } else {
          val wordcount = model.get_item(word)
          //if (wordcount <= 0 or model.num_tokens <= 0 or unseen_mass >= 1.0)
          //  warning("Bad values; wordcount = %s, unseen_mass = %s",
          //          wordcount, unseen_mass)
          //  for ((word, count) <- self.counts)
          //    errprint("%s: %s", word, count)
          val wordprob = wordcount.toDouble/normalization_factor*(1.0 - unseen_mass)
          if (debug("lots"))
            errprint("Word %s, seen in document, wordprob = %s",
                     unmemoize_string(word), wordprob)
          wordprob
        }
      retval
    }
  }
}

