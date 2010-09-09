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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.ec.util.MersenneTwisterFast;
import opennlp.textgrounder.bayesian.mathutils.*;
import opennlp.textgrounder.bayesian.spherical.annealers.*;
import opennlp.textgrounder.bayesian.spherical.io.*;
import opennlp.textgrounder.bayesian.structs.*;
import opennlp.textgrounder.bayesian.utils.TGArrays;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class SphericalModelBase extends SphericalModelFields {

    protected final static double EXPANSION_FACTOR = 0.25;
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
    protected transient SphericalInputReader inputReader;
    /**
     * 
     */
    protected transient SphericalOutputWriter outputWriter;
    /**
     * 
     */
    protected transient SphericalAnnealer annealer;
    /**
     * the crpalpha for use when spherical distributions are not normalized
     */
    protected transient double crpalpha_mod;

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
                inputReader = new SphericalBinaryInputReader(_experimentParameters);
                break;
            case TEXT:
                inputReader = new SphericalTextInputReader(_experimentParameters);
                break;
        }

        crpalpha = _experimentParameters.getCrpalpha();
        alpha = _experimentParameters.getAlpha();
        beta = _experimentParameters.getBeta();
        kappa = _experimentParameters.getKappa();
        crpalpha_mod = crpalpha * 4 * Math.PI * Math.sinh(kappa) / kappa;

        Z = _experimentParameters.getTopics();

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
        if (Math.abs(initialTemp - targetTemp) < SphericalAnnealer.EPSILON) {
            annealer = new SphericalEmptyAnnealer(_experimentParameters);
        } else {
            annealer = new SphericalSimulatedAnnealer(_experimentParameters);
        }

        readTokenArrayFile();
        readRegionCoordinateList();
    }

    public void initialize() {
        initialize(experimentParameters);

        expectedR = (int) Math.ceil(crpalpha * Math.log(1 + N / crpalpha)) * 2;

        toponymRegionCounts = new int[expectedR];
        Arrays.fill(toponymRegionCounts, 0);

        allWordsRegionCounts = new int[expectedR];
        Arrays.fill(allWordsRegionCounts, 0);

        regionByDocumentCounts = new int[D * expectedR];
        Arrays.fill(regionByDocumentCounts, 0);

        wordByRegionCounts = new int[W * expectedR];
        Arrays.fill(wordByRegionCounts, 0);

        regionMeans = new double[expectedR][];

        regionToponymCoordinateCounts = new int[expectedR][][];
        for (int i = 0; i < expectedR; ++i) {
            int[][] toponymCoordinateCounts = new int[T][];
            for (int j = 0; j < T; ++j) {
                int coordinates = toponymCoordinateLexicon[j].length;
                int[] coordcounts = new int[coordinates];
                Arrays.fill(coordcounts, 0);
                toponymCoordinateCounts[j] = coordcounts;
            }
            regionToponymCoordinateCounts[i] = toponymCoordinateCounts;
        }
    }

    protected void readTokenArrayFile() {

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
                    if (stopstatus == 0) {
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
        betaW = beta * W;
        D += 1;
        N = wordArray.size();

        wordVector = new int[N];
        copyToArray(wordVector, wordArray);

        documentVector = new int[N];
        copyToArray(documentVector, docArray);

        toponymVector = new int[N];
        copyToArray(toponymVector, toponymArray);

        stopwordVector = new int[N];
        copyToArray(stopwordVector, stopwordArray);

        regionVector = new int[N];
        coordinateVector = new int[N];
        for (int i = 0; i < N; ++i) {
            regionVector[i] = -1;
            coordinateVector[i] = -1;
        }
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
        toponymCoordinateLexicon = new double[T][][];
        maxCoord = 0;

        for (Entry<Integer, double[]> entry : toprecords.entrySet()) {
            int topid = entry.getKey();
            double[] sphericalrecord = entry.getValue();
            double[][] cartesianrecords = new double[sphericalrecord.length / 2][];
            for (int i = 0; i < sphericalrecord.length / 2; i++) {
                double[] crec = TGMath.sphericalToCartesian(sphericalrecord[2 * i], sphericalrecord[2 * i + 1]);
                cartesianrecords[i] = crec;
            }
            toponymCoordinateLexicon[topid] = cartesianrecords;
            if (cartesianrecords.length > maxCoord) {
                maxCoord = cartesianrecords.length;
            }
        }
        maxCoord += 1;
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    public abstract void randomInitialize();

    /**
     * Train topics
     *
     * @param decoder Annealing scheme to use
     */
    public abstract void train(SphericalAnnealer _annealer);

    protected abstract void expandExpectedR();

    protected void resetRegionID(SphericalAnnealer _annealer, int curregionid, int curdocid) {
        double[] probs = new double[currentR];
        allWordsRegionCounts[curregionid] = 0;
        for (int i = 0; i < D; ++i) {
            regionByDocumentCounts[i * expectedR + curregionid] = 0;
        }
        for (int i = 0; i < W; ++i) {
            wordByRegionCounts[i * expectedR + curregionid] = 0;
        }

        for (int i = 0; i < N; ++i) {
            if (regionVector[i] == curregionid) {
                if (stopwordVector[i] == 0 && toponymVector[i] == 0) {
                    int wordid = wordVector[i];
                    int docid = documentVector[i];
                    int docoff = docid * expectedR;
                    int wordoff = wordid * expectedR;
                    for (int j = 0; j < currentR; ++j) {
                        probs[j] = (wordByRegionCounts[wordoff + j] + beta)
                              / (allWordsRegionCounts[j] + betaW)
                              * regionByDocumentCounts[docoff + j];
                    }
                    probs[curregionid] = 0;
                    double totalprob = annealer.annealProbs(0, currentR, probs);
                    double r = rand.nextDouble() * totalprob;
                    double max = probs[0];
                    int regionid = 0;
                    while (r > max) {
                        regionid++;
                        max += probs[regionid];
                    }
                    regionVector[i] = regionid;

                    allWordsRegionCounts[regionid]++;
                    regionByDocumentCounts[docoff + regionid]++;
                    wordByRegionCounts[wordoff + regionid]++;
                }
            }
        }
    }

    public void train() {
        System.err.println(String.format("Randomly initializing with %d tokens, %d words, %d documents, and %d expected regions", N, W, D, expectedR));
        randomInitialize();
        System.err.println(String.format("Beginning training with %d tokens, %d words, %d documents, and %d expected regions", N, W, D, expectedR));
        train(annealer);
        if (annealer.getSamples() != 0) {
            averagedWordByRegionCounts = annealer.getWordByRegionCounts();
            averagedAllWordsRegionCounts = annealer.getAllWordsRegionCounts();
            averagedRegionByDocumentCounts = annealer.getRegionByDocumentCounts();
            averagedRegionMeans = annealer.getRegionMeans();
            averagedRegionToponymCoordinateCounts = annealer.getRegionToponymCoordinateCounts();
        }
    }

    public abstract void decode();

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
        outputWriter = new SphericalBinaryOutputWriter(experimentParameters);
        outputWriter.writeTokenArray(wordVector, documentVector, toponymVector, stopwordVector, regionVector, coordinateVector);

        AveragedSphericalCountWrapper averagedSphericalCountWrapper = new AveragedSphericalCountWrapper(this);
        averagedSphericalCountWrapper.addHyperparameters();

        outputWriter.writeProbabilities(averagedSphericalCountWrapper);
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
