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
package opennlp.textgrounder.bayesian.spherical.models;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.ec.util.MersenneTwisterFast;
import opennlp.textgrounder.bayesian.mathutils.TGMath;
import opennlp.textgrounder.bayesian.rlda.annealers.*;
import opennlp.textgrounder.bayesian.spherical.io.*;
import opennlp.textgrounder.bayesian.structs.*;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalModelBase extends SphericalModelFields {

    /**
     * Random number generator. Implements the fast Mersenne Twister.
     */
    protected transient MersenneTwisterFast rand;
    /**
     * 
     */
    protected transient ExperimentParameters experimentParameters;
    /**
     * 
     */
    protected transient InputReader inputReader;
    /**
     * 
     */
    protected transient OutputWriter outputWriter;
    /**
     * 
     */
    protected transient Annealer annealer;

    /**
     * Default constructor. Take input from commandline and default _options
     * and initialize class. Also, process input text and process so that
     * toponyms, stopwords and other words are identified and collected.
     *
     * @param _options
     */
    public SphericalModelBase(ExperimentParameters _parameters) {
        experimentParameters = _parameters;
    }

    /**
     *
     * @param _options
     */
    protected void initialize(ExperimentParameters _experimentParameters) {

        switch (_experimentParameters.getInputFormat()) {
            case BINARY:
                inputReader = new BinaryInputReader(_experimentParameters);
                break;
            case TEXT:
                inputReader = new TextInputReader(_experimentParameters);
                break;
        }

        alpha = _experimentParameters.getAlpha();
        beta = _experimentParameters.getBeta();
        betaW = beta * W;
        kappa = _experimentParameters.getKappa();

        int randSeed = _experimentParameters.getRandomSeed();
        if (randSeed == 0) {
            /**
             * Case for complete random seeding
             */
            rand = new MersenneTwisterFast();
        } else {
            /**
             * Case for non-random seeding. For debugging. Also, the default
             */
            rand = new MersenneTwisterFast(randSeed);
        }

        double targetTemp = _experimentParameters.getTargetTemperature();
        double initialTemp = _experimentParameters.getInitialTemperature();
        if (Math.abs(initialTemp - targetTemp) < Annealer.EPSILON) {
            annealer = new EmptyAnnealer(_experimentParameters);
        } else {
            annealer = new SimulatedAnnealer(_experimentParameters);
        }

        readTokenArrayFile();
        readRegionCoordinateList();
    }

    public void initialize() {
        initialize(experimentParameters);

        expectedR = (int) Math.ceil(alpha * Math.log(1 + N / alpha)) * 2;

        regionCounts = new int[expectedR];
        for (int i = 0; i < expectedR; ++i) {
            regionCounts[i] = 0;
        }
        regionByDocumentCounts = new int[D * expectedR];
        for (int i = 0; i < D * expectedR; ++i) {
            regionByDocumentCounts[i] = 0;
        }
        wordByRegionCounts = new int[W * expectedR];
        for (int i = 0; i < W * expectedR; ++i) {
            wordByRegionCounts[i] = 0;
        }
        toponymByRegionCounts = new int[T * expectedR];
        for (int i = 0; i < T * expectedR; ++i) {
            toponymByRegionCounts[i] = 0;
        }
        regionMeans = new double[expectedR][];
        for (int i = 0; i < expectedR; ++i) {
            double[] means = new double[3];
            for (int j = 0; j < 3; ++j) {
                means[j] = 0;
            }
            regionMeans[i] = means;
        }

        regionToponymCoordinateCounts = new int[expectedR][][];
        for (int i = 0; i < expectedR; ++i) {
            int[][] toponymCoordinateCounts = new int[T][];
            for (int j = 0; j < T; ++j) {
                int coordinates = toponymCoordinateLexicon[j].length;
                int[] coordcounts = new int[coordinates];
                for (int k = 0; k < coordinates; ++k) {
                    coordcounts[k] = 0;
                }
                toponymCoordinateCounts[j] = coordcounts;
            }
            regionToponymCoordinateCounts[i] = toponymCoordinateCounts;
        }
    }

    protected void readTokenArrayFile() {

        HashSet<Integer> stopwordSet = new HashSet<Integer>();
        ArrayList<Integer> wordArray = new ArrayList<Integer>(),
              docArray = new ArrayList<Integer>(),
              toponymArray = new ArrayList<Integer>(),
              stopwordArray = new ArrayList<Integer>();

        try {
            while (true) {
                int[] record = inputReader.nextTokenArrayRecord();
                if (record != null) {
                    int wordid = record[0];
                    wordArray.add(wordid);
                    int docid = record[1];
                    docArray.add(docid);
                    int topstatus = record[2];
                    toponymArray.add(topstatus);
                    int stopstatus = record[3];
                    stopwordArray.add(stopstatus);
                    if (stopstatus == 1) {
                        stopwordSet.add(wordid);
                    } else {
                        if (W < wordid) {
                            W = wordid;
                        }
                    }
                    if (D < docid) {
                        D = docid;
                    }
                }
            }
        } catch (EOFException ex) {
        } catch (IOException ex) {
            Logger.getLogger(SphericalModelBase.class.getName()).log(Level.SEVERE, null, ex);
        }

        W += 1;
        D += 1;
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

    /**
     * 
     * @param _file
     */
    public void readRegionCoordinateList() {

        HashMap<Integer, double[]> toprecords = new HashMap<Integer, double[]>();
        int maxtopid = 0;

        try {
            while (true) {
                ArrayList<Object> toprecord = inputReader.nextToponymCoordinateRecord();

                int topid = (Integer) toprecord.get(0);
                double[] record = (double[]) toprecord.get(1);

                toprecords.put(topid, record);
                if (topid > maxtopid) {
                    maxtopid = topid;
                }
            }
        } catch (EOFException e) {
        } catch (IOException ex) {
            Logger.getLogger(SphericalModelBase.class.getName()).log(Level.SEVERE, null, ex);
        }

        T = maxtopid + 1;
        toponymCoordinateLexicon = new double[T][];

        for (Entry<Integer, double[]> entry : toprecords.entrySet()) {
            int topid = entry.getKey();
            double[] sphericalrecord = entry.getValue();
            double[] cartesianrecord = new double[sphericalrecord.length / 2 * 3];
            int[] coordcounts = new int[sphericalrecord.length / 2];
            for (int i = 0; i < sphericalrecord.length / 2; i++) {
                double[] crec = TGMath.sphericalToCartesian(sphericalrecord[2 * i], sphericalrecord[2 * i + 1]);
                for (int j = 0; j < 3; ++j) {
                    cartesianrecord[3 * i + j] = crec[j];
                }
                coordcounts[i] = 0;
            }
            toponymCoordinateLexicon[topid] = cartesianrecord;
        }
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    public void randomInitialize() {
        currentR = 0;
        int wordid, docid, regionid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] probs = new double[expectedR];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                docoff = docid * expectedR;
                wordoff = wordid * expectedR;
                istoponym = toponymVector[i];

                totalprob = 0;
                for (int j = 0; j < currentR; ++j) {
                    totalprob += probs[j] = 1;
                }

                r = rand.nextDouble() * totalprob + alpha / 2;
                probs[currentR] = alpha / 2;

                max = probs[0];
                regionid = 0;
                while (r > max) {
                    regionid++;
                    max += probs[regionid];
                }
                if (regionid == currentR) {
                    currentR += 1;
                }

                regionVector[i] = regionid;
                regionCounts[regionid]++;
                regionByDocumentCounts[docoff + regionid]++;
                wordByRegionCounts[wordoff + regionid]++;

                if (istoponym == 1) {
                    toponymByRegionCounts[wordoff + regionid]++;
                    int coordinates = toponymCoordinateLexicon[wordid].length / 3;
                    int coordid = rand.nextInt(coordinates);
                    regionToponymCoordinateCounts[regionid][wordid][coordid] += 1;
                }
            }
        }
    }

    protected void calculateMeans() {
        double[] mean = new double[3];
        for (int i = 0; i < 3; ++i) {
            mean[i] = 0;
        }

        for (int i = 0; i < currentR; ++i) {
            for (int j = 0; j < T; ++j) {
                int[] coordcounts = _toponymCoordinateCounts[j];
                double[] coords = _toponymCoordinateLexicon[j];
                for (int k = 0; k < coordcounts.length; ++k) {
                    int count = coordcounts[k];
                    for (int l = 0; l < 3; ++l) {
                        mean[l] += count * coords[3 * k + l];
                    }
                }
            }
        }

        double norm = TGMath.l2Norm(mean);
        for (int i = 0; i < 3; ++i) {
            mean[i] /= norm;
        }
    }

    /**
     * Train topics
     *
     * @param decoder Annealing scheme to use
     */
    public void train(Annealer _annealer) {
        int wordid, docid, regionid;
        int wordoff, docoff;
        int istoponym, isstopword;
        double[] probs = new double[expectedR];
        double totalprob, max, r;

        while (_annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                if (isstopword == 0) {
                    wordid = wordVector[i];
                    docid = documentVector[i];
                    regionid = regionVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * expectedR;
                    wordoff = wordid * expectedR;

                    regionCounts[regionid]--;
                    regionByDocumentCounts[docoff + regionid]--;
                    wordByRegionCounts[wordoff + regionid]--;

                    try {
                        if (istoponym == 1) {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByRegionCounts[wordoff + j] + beta)
                                      / (regionCounts[j] + betaW)
                                      * (regionByDocumentCounts[docoff + j] + alpha)
                                      * toponymCoordinateLexicon[wordoff + j];
//                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByRegionCounts[wordoff + j] + beta)
                                      / (regionCounts[j] + betaW)
                                      * (regionByDocumentCounts[docoff + j] + alpha);
                            }
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                    totalprob = _annealer.annealProbs(probs);
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

            _annealer.collectSamples(regionCounts, wordByRegionCounts, regionByDocumentCounts);
        }
    }

    public void train() {
        System.err.println(String.format("Randomly initializing with %d tokens, %d words, %d documents, and %d expected regions", N, W, D, expectedR));
        randomInitialize();
        System.err.println(String.format("Beginning training with %d tokens, %d words, %d documents, and %d expected regions", N, W, D, expectedR));
        train(annealer);
        if (annealer.getSamples() != 0) {
            normalizedRegionCounts = annealer.getNormalizedTopicSampleCounts();
            normalizedWordByRegionCounts = annealer.getNormalizedWordByTopicSampledProbs();
            normalizedRegionByDocumentCounts = annealer.getNormalizedRegionByDocumentSampledCounts();
        }
    }

    public void decode() {
        System.err.println(String.format("Decoding maximum posterior topics"));
        Annealer decoder = new MaximumPosteriorDecoder();
        int wordid, docid, regionid;
        int wordoff, docoff;
        int istoponym, isstopword;
        double[] probs = new double[R];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                regionid = regionVector[i];
                istoponym = toponymVector[i];
                docoff = docid * R;
                wordoff = wordid * R;

                try {
                    if (istoponym == 1) {
                        for (int j = 0;; ++j) {
                            probs[j] = (normalizedWordByRegionCounts[wordoff + j] + beta)
                                  / (normalizedRegionCounts[j] + betaW)
                                  * (normalizedRegionByDocumentCounts[docoff + j] + alpha)
                                  * toponymCoordinateLexicon[wordoff + j];
//                                      * activeRegionByDocumentFilter[docoff + j];
                        }
                    } else {
                        for (int j = 0;; ++j) {
                            probs[j] = (normalizedWordByRegionCounts[wordoff + j] + beta)
                                  / (normalizedRegionCounts[j] + betaW)
                                  * (normalizedRegionByDocumentCounts[docoff + j] + alpha);
                        }
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                }
                totalprob = decoder.annealProbs(probs);
                r = rand.nextDouble() * totalprob;

                max = probs[0];
                regionid = 0;
                while (r > max) {
                    regionid++;
                    max += probs[regionid];
                }
                regionVector[i] = regionid;
            }
        }
    }

    public void normalize() {
        throw new UnsupportedOperationException("Normalization not a valid operation in this program");
    }

    /**
     *
     * @param _outputFilename
     * @throws IOException
     */
    public void saveModel(String _outputFilename) throws IOException {
        ObjectOutputStream modelOut =
              new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(_outputFilename + ".gz")));
        modelOut.writeObject(this);
        modelOut.close();
    }

    public void write() {
        outputWriter = new BinaryOutputWriter(experimentParameters);
        outputWriter.writeTokenArray(wordVector, documentVector, toponymVector, stopwordVector, regionVector);

        NormalizedProbabilityWrapper normalizedProbabilityWrapper = new NormalizedProbabilityWrapper(this);
        normalizedProbabilityWrapper.addHyperparameters();

        outputWriter.writeProbabilities(normalizedProbabilityWrapper);
    }

    /**
     * Copy a sequence of numbers from ta to array ia.
     *
     * @param <T>   Any number type
     * @param ia    Target array of integers to be copied to
     * @param ta    Source List<T> of numbers to be copied from
     */
    protected static <T extends Number> void copyToArray(int[] ia,
          ArrayList<T> ta) {
        for (int i = 0; i < ta.size(); ++i) {
            ia[i] = ta.get(i).intValue();
        }
    }
}
