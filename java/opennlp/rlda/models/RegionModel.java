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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import opennlp.rlda.annealers.*;
import opennlp.rlda.apps.ExperimentParameters;
import opennlp.rlda.ec.util.MersenneTwisterFast;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class RegionModel implements Serializable {

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
    protected transient int[] regionVector;
    /**
     * Counts of topics
     */
    protected transient int[] regionCounts;
    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected transient int[] wordByRegionCounts;
    /**
     * Counts of topics per document
     */
    protected transient int[] regionByDocumentCounts;
    /**
     * Probability of word given topic. since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected transient float[] wordByRegionProbs;
    /**
     * Hyperparameter for region*doc priors
     */
    protected transient double alpha;
    /**
     * Hyperparameter for word*region priors
     */
    protected transient double beta;
    /**
     * Normalization term for word*region gibbs sampler
     */
    protected transient double betaW;
    /**
     * Number of regions
     */
    protected transient int R;
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
    protected transient float[] regionProbs;
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
     * frugality with memory. The dimensions are equivalent to the wordByRegionCounts
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
    public RegionModel(ExperimentParameters _parameters) {
        try {
            initialize(_parameters);
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
    protected final void initialize(ExperimentParameters _parameters) throws
          FileNotFoundException, IOException {
        alpha = _parameters.getAlpha();
        beta = _parameters.getBeta();
    }

    public void readTokenFile(File _file) throws FileNotFoundException,
          IOException {
        BufferedReader textIn = new BufferedReader(new FileReader(_file));

        HashSet<Integer> stopwordSet = new HashSet<Integer>();
        String line = null;
        ArrayList<Integer> wordArray = new ArrayList<Integer>(),
              docArray = new ArrayList<Integer>(),
              toponymArray = new ArrayList<Integer>(),
              stopwordArray = new ArrayList<Integer>();

        while ((line = textIn.readLine()) != null) {
            String[] fields = line.split("\\w+");
            if (fields.length > 2) {
                int wordidx = Integer.parseInt(fields[0]);
                wordArray.add(wordidx);
                int docidx = Integer.parseInt(fields[1]);
                docArray.add(docidx);
                toponymArray.add(Integer.parseInt(fields[2]));
                try {
                    stopwordArray.add(Integer.parseInt(fields[3]));
                    stopwordSet.add(wordidx);
                } catch (ArrayIndexOutOfBoundsException e) {
                }

                if (W < wordidx) {
                    W = wordidx;
                }
                if (D < docidx) {
                    D = docidx;
                }
            }
        }

        W -= stopwordSet.size();
        N = wordArray.size();

        wordVector = new int[N];
        copyToArray(wordVector, wordArray);

        documentVector = new int[N];
        copyToArray(documentVector, docArray);

        toponymVector = new int[N];
        copyToArray(toponymVector, toponymArray);

        stopwordVector = new int[N];
        if (stopwordArray.size() == N) {
            copyToArray(stopwordVector, stopwordArray);
        } else {
            for (int i = 0; i < N; ++i) {
                stopwordVector[i] = 0;
            }
        }

        regionVector = new int[N];
    }

    public void readRegionToponymFilter(File _file) throws FileNotFoundException,
          IOException {
        BufferedReader textin = new BufferedReader(new FileReader(_file));

        String line = null;

        regionByToponymFilter = new int[R * W];
        for (int i = 0; i < R * W; ++i) {
            regionByToponymFilter[i] = 0;
        }

        while ((line = textin.readLine()) != null) {
            if (!line.isEmpty()) {
                String[] fields = line.split("\\w+");

                int wordoff = Integer.parseInt(fields[0]) * R;
                for (int i = 1; i < fields.length; ++i) {
                    regionByToponymFilter[wordoff + i] = 1;
                }
            }
        }
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    public void randomInitialize() {
        int wordid, docid, regionid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] probs = new double[R];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                docoff = docid * R;
                istoponym = toponymVector[i];

                totalprob = 0;
                if (istoponym == 1) {
                    wordoff = wordid * R;
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
                regionid = 0;
                while (r > max) {
                    regionid++;
                    max += probs[regionid];
                }

                regionVector[i] = regionid;
                regionCounts[regionid]++;
                regionByDocumentCounts[docid * R + regionid]++;
                wordByRegionCounts[wordid * R + regionid]++;
            }
        }
    }

    /**
     * Train topics
     *
     * @param annealer Annealing scheme to use
     */
    public void train(Annealer annealer) {
        int wordid, docid, regionid;
        int wordoff, docoff;
        int istoponym, isstopword;
        double[] probs = new double[R];
        double totalprob, max, r;

        while (annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                if (isstopword == 0) {
                    wordid = wordVector[i];
                    docid = documentVector[i];
                    regionid = regionVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * R;
                    wordoff = wordid * R;

                    regionCounts[regionid]--;
                    regionByDocumentCounts[docoff + regionid]--;
                    wordByRegionCounts[wordoff + regionid]--;

                    try {
                        if (istoponym == 1) {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByRegionCounts[wordoff + j] + beta)
                                      / (regionCounts[j] + betaW)
                                      * (regionByDocumentCounts[docoff + j] + alpha)
                                      * regionByToponymFilter[wordoff + j]
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByRegionCounts[wordoff + j] + beta)
                                      / (regionCounts[j] + betaW)
                                      * (regionByDocumentCounts[docoff + j] + alpha)
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                    totalprob = annealer.annealProbs(probs);
                    r = rand.nextDouble() * totalprob;

                    max = probs[0];
                    regionid = 0;
                    while (r > max) {
                        regionid++;
                        max += probs[regionid];
                    }
                    regionVector[i] = regionid;

                    regionCounts[regionid]++;
                    regionByDocumentCounts[docoff + regionid]++;
                    wordByRegionCounts[wordoff + regionid]++;
                }
            }

            annealer.collectSamples(regionCounts, wordByRegionCounts);
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

    /**
     * Copy a sequence of numbers from ta to array ia.
     *
     * @param <T>   Any number type
     * @param ia    Target array of integers to be copied to
     * @param ta    Source List<T> of numbers to be copied from
     */
    protected static <T extends Number> void copyToArray(int[] ia, List<T> ta) {
        for (int i = 0; i < ta.size(); ++i) {
            ia[i] = ta.get(i).intValue();
        }
    }
}
