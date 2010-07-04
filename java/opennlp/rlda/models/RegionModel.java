///////////////////////////////////////////////////////////////////////////////
//  Copyright 2010 Taesun Moon <tsunmoon@gmail.com>.
// 
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.rlda.models;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.rlda.annealers.*;
import opennlp.rlda.apps.CommandLineOptions;
import opennlp.rlda.ec.util.MersenneTwisterFast;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class RegionModel implements Serializable{

    /**
     * Random number generator. Implements the fast Mersenne Twister.
     */
    protected transient MersenneTwisterFast rand;
    /**
     * Vector of document indices
     */
    protected transient int[] documentVector;
    /**
     * Vector of word indices
     */
    protected transient int[] wordVector;
    /**
     * Vector of topics
     */
    protected transient int[] topicVector;
    /**
     * Counts of topics
     */
    protected transient int[] topicCounts;
    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected transient int[] wordByTopicCounts;
    /**
     * Counts of topics per document
     */
    protected transient int[] topicByDocumentCounts;
    /**
     * Probability of word given topic. since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected transient float[] wordByTopicProbs;
    /**
     * Hyperparameter for topic*doc priors
     */
    protected transient double alpha;
    /**
     * Hyperparameter for word*topic priors
     */
    protected transient double beta;
    /**
     * Normalization term for word*topic gibbs sampler
     */
    protected transient double betaW;
    /**
     * Number of topics
     */
    protected transient int T;
    /**
     * Number of non-stopword word types. Equivalent to <p>fW-sW</p>.
     */
    protected transient int W;
    /**
     * Size of the vocabulary including stopwords.
     */
    protected transient int fW;
    /**
     * Size of stopword list
     */
    protected transient int sW;
    /**
     * Number of documents
     */
    protected transient int D;
    /**
     * Number of tokens
     */
    protected transient int N;
    /**
     * Handles simulated annealing, burn-in, and full sampling cycle
     */
    protected transient Annealer annealer;
    /**
     * Posterior probabilities for topics.
     */
    protected transient float[] topicProbs;
    /**
     * Output buffer to write normalized, tabulated data to.
     */
    protected transient BufferedWriter tabularOutput;
    /**
     * Name of output buffer to write normalized, tabulated data to
     */
    protected transient String tabularOutputFilename;
    /**
     * Vector of toponyms. If 0, the word is not a toponym. If 1, it is.
     */
    protected transient int[] toponymVector;
    /**
     * Vector of stopwords. If 0, the word is not a stopword. If 1, it is.
     */
    protected transient int[] stopwordVector;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByTopicCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected transient int[] regionByToponymFilter;
    /**
     *
     */
    protected transient int evalIterations;
    /**
     *
     */
    protected transient int[] wordIdMapper;
    /**
     *
     */
    protected transient int[] activeRegionByDocumentFilter;

    /**
     * Default constructor. Take input from commandline and default _options
     * and initialize class. Also, process input text and process so that
     * toponyms, stopwords and other words are identified and collected.
     *
     * @param _options
     */
    public RegionModel(CommandLineOptions _options) {
        try {
            initialize(_options);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(RegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RegionModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @param _options
     */
    protected final void initialize(CommandLineOptions _options) throws
          FileNotFoundException, IOException {
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    public void randomInitialize() {
        int wordid, docid, topicid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordIdMapper[wordVector[i]];
                docid = documentVector[i];
                docoff = docid * T;
                istoponym = toponymVector[i];

                totalprob = 0;
                if (istoponym == 1) {
                    wordoff = wordid * T;
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] =
                                  regionByToponymFilter[wordoff + j]
                                  * activeRegionByDocumentFilter[docoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                } else {
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] =
                                  activeRegionByDocumentFilter[docoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                }

                r = rand.nextDouble() * totalprob;

                max = probs[0];
                topicid = 0;
                while (r > max) {
                    topicid++;
                    max += probs[topicid];
                }

                topicVector[i] = topicid;
                topicCounts[topicid]++;
                topicByDocumentCounts[docid * T + topicid]++;
                wordByTopicCounts[wordid * T + topicid]++;
            }
        }
    }

    /**
     * Train topics
     *
     * @param annealer Annealing scheme to use
     */
    public void train(Annealer annealer) {
        int wordid, docid, topicid;
        int wordoff, docoff;
        int istoponym, isstopword;
        double[] probs = new double[T];
        double totalprob, max, r;

        while (annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                if (isstopword == 0) {
                    wordid = wordIdMapper[wordVector[i]];
                    docid = documentVector[i];
                    topicid = topicVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * T;
                    wordoff = wordid * T;

                    topicCounts[topicid]--;
                    topicByDocumentCounts[docoff + topicid]--;
                    wordByTopicCounts[wordoff + topicid]--;

                    try {
                        if (istoponym == 1) {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha)
                                      * regionByToponymFilter[wordoff + j]
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha)
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                    totalprob = annealer.annealProbs(probs);
                    r = rand.nextDouble() * totalprob;

                    max = probs[0];
                    topicid = 0;
                    while (r > max) {
                        topicid++;
                        max += probs[topicid];
                    }
                    topicVector[i] = topicid;

                    topicCounts[topicid]++;
                    topicByDocumentCounts[docoff + topicid]++;
                    wordByTopicCounts[wordoff + topicid]++;
                }
            }

            annealer.collectSamples(topicCounts, wordByTopicCounts);
        }
    }

    /**
     * Remove stopwords from normalization process
     */
    public void normalize() {
    }

    /**
     *
     * @param outputFilename
     * @throws IOException
     */
    public void saveSimpleParameters(String outputFilename) throws IOException {
    }

    /**
     *
     * @param inputFilename
     * @throws IOException
     */
    public void loadSimpleParameters(String inputFilename) throws IOException {
    }
}
