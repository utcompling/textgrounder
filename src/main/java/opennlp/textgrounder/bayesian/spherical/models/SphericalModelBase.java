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

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class SphericalModelBase extends SphericalModelFields {

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
                inputReader = new SphericalGlobalToInternalBinaryInputReader(_experimentParameters);
                break;
            case TEXT:
                inputReader = new SphericalTextInputReader(_experimentParameters);
                break;
        }

        alpha_H = _experimentParameters.getAlpha();
//        kappa = _experimentParameters.getKappa();

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
    }

    public void initialize() {
        initialize(experimentParameters);
        readTokenArrayFile();
        readRegionCoordinateList();
        initializeCountArrays();
    }

    /**
     * The following class variables are set in this procedure
     *
     * <p>
     * expectedR
     * </p>
     *
     * The following class variables are initialized in this procedure
     *
     * <p>
     * toponymRegionCounts
     * allWordsRegionCounts
     * regionByDocumentCounts
     * wordByRegionCounts
     * regionMeans
     * regionToponymCoordinateCounts
     * </p>
     */
    protected void initializeCountArrays() {
        dishCountsOfToponyms = new int[L];
        Arrays.fill(dishCountsOfToponyms, 0);

        regionCountsOfAllWords = new int[L];
        Arrays.fill(regionCountsOfAllWords, 0);

        regionByDocumentCounts = new int[D * L];
        Arrays.fill(regionByDocumentCounts, 0);

        regionMeans = new double[L][];

        toponymCoordinateCounts = new int[T][];
        for (int j = 0; j < T; ++j) {
            int coordinates = toponymCoordinateLexicon[j].length;
            int[] coordcounts = new int[coordinates];
            Arrays.fill(coordcounts, 0);
            toponymCoordinateCounts[j] = coordcounts;
        }

    }

    /**
     * The following class variables are set in this procedure
     * <p>
     * W
     * D
     * betaW
     * N
     * </p>
     *
     * The following class variables are initialized in this procedure
     * <p>
     * wordVector
     * documentVector
     * toponymVector
     * stopwordVector
     * regionVector
     * coordinateVector
     * </p>
     */
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
        Arrays.fill(regionVector, -1);

        coordinateVector = new int[N];
        Arrays.fill(coordinateVector, -1);
    }

    /**
     * The following class variables are set in this procedure
     *
     * <p>
     * T
     * toponymCoordinateLexicon
     * maxCoord
     * </p>
     *
     * @param _file
     */
    public void readRegionCoordinateList() {

        HashMap<Integer, double[]> toprecords = new HashMap<Integer, double[]>();
        T = 0;

        try {
            while (true) {
                ArrayList<Object> toprecord = inputReader.nextToponymCoordinateRecord();

                int topid = (Integer) toprecord.get(0);
                double[] record = (double[]) toprecord.get(1);

                toprecords.put(topid, record);
                if (topid > T) {
                    T = topid;
                }
            }
        } catch (EOFException e) {
        } catch (IOException ex) {
            Logger.getLogger(SphericalModelBase.class.getName()).log(Level.SEVERE, null, ex);
        }

        T += 1;
        toponymCoordinateLexicon = new double[T][][];
        maxCoord = 0;

        for (Entry<Integer, double[]> entry : toprecords.entrySet()) {
            int topid = entry.getKey();
            double[] sphericalrecord = entry.getValue();
            double[][] cartesianrecords = new double[sphericalrecord.length / 2][];
            for (int i = 0; i < sphericalrecord.length / 2; i++) {
                double[] crec = TGMath.sphericalToCartesian(TGMath.geographicToSpherical(sphericalrecord[2 * i], sphericalrecord[2 * i + 1]));
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

    /**
     * 
     */
    public abstract void decode();

    public void train() {
        System.err.println(String.format("Randomly initializing with %d tokens, %d words, %d documents, and %d expected regions", N, W, D, L));
        randomInitialize();
        System.err.println(String.format("Beginning training with %d tokens, %d words, %d documents, and %d expected regions", N, W, D, L));
        train(annealer);
        if (annealer.getSamples() != 0) {
            globalDishWeightsFM = annealer.getGlobalDishWeightsFM();
            localDishWeightsFM = annealer.getLocalDishWeightsFM();
            kappaFM = annealer.getKappaFM();
            regionMeansFM = annealer.getRegionMeansFM();
            toponymCoordinateDirichletFM = annealer.getToponymCoordinateWeightsFM();
        }
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
        outputWriter = new SphericalBinaryOutputWriter(experimentParameters);
        outputWriter.writeTokenArray(wordVector, documentVector, toponymVector, stopwordVector, regionVector, coordinateVector);

        AveragedSphericalCountWrapper averagedSphericalCountWrapper = new AveragedSphericalCountWrapper(this);
//        averagedSphericalCountWrapper.addHyperparameters();

        outputWriter.writeProbabilities(averagedSphericalCountWrapper);
    }

    //////////////////////////////////////////////////////////////////////
    /**
     * parameter update routines
     */
    //////////////////////////////////////////////////////////////////////
    /**
     * 
     * @param _mu
     * @return
     */
    public double evalMuLogLikelihood(double[] _mu, int _l) {
        double logLikelihood = 0;
        for (int i = 0; i < N; ++i) {
            int istoponym = toponymVector[i];
            if (istoponym == 1) {
                if (regionVector[i] == _l) {
                    int coordid = coordinateVector[i];
                    int wordid = wordVector[i];
                    double[] coord = toponymCoordinateLexicon[wordid][coordid];
                    double likelihood = TGBLAS.ddot(0, coord, 1, _mu, 1);
                    logLikelihood += likelihood;
                }
            }
        }
        return logLikelihood;
    }

    public double evalKappaLogLikelihood(double _kappa, double[] _mu, int _l) {
        double logLikelihood = 0;
        int lcount = 0;
        if (_kappa > 5) {
            for (int i = 0; i < N; ++i) {
                int istoponym = toponymVector[i];
                if (istoponym == 1) {
                    if (regionVector[i] == _l) {
                        lcount += 1;
                        int coordid = coordinateVector[i];
                        int wordid = wordVector[i];
                        double[] coord = toponymCoordinateLexicon[wordid][coordid];
                        logLikelihood += TGBLAS.ddot(0, coord, 1, _mu, 1);
                    }
                }
            }
            logLikelihood = lcount * (Math.log(0.5 * _kappa / Math.PI) - _kappa) + _kappa * logLikelihood;
        } else {
            for (int i = 0; i < N; ++i) {
                int istoponym = toponymVector[i];
                if (istoponym == 1) {
                    if (regionVector[i] == _l) {
                        lcount += 1;
                        int coordid = coordinateVector[i];
                        int wordid = wordVector[i];
                        double[] coord = toponymCoordinateLexicon[wordid][coordid];
                        logLikelihood += TGBLAS.ddot(0, coord, 1, _mu, 1);
                    }
                }
            }
            logLikelihood = lcount * (Math.log(_kappa / (4 * Math.PI * Math.sinh(_kappa)))) + _kappa * logLikelihood;
        }
        return logLikelihood;
    }

    public double globalAlphaUpdate() {
        int Ls = 0;
        for (int d : globalDishCounts) {
            if (d > 0) {
                Ls++;
            }
        }

        double q = RKRand.rk_beta(alpha_H + 1, Nn);
        double pq = (alpha_h_d + Ls - 1) / (Nn * (alpha_h_f - Math.log(q)));
        int s = 0;
        if (rand.nextDouble() < pq) {
            s = 1;
        }
        double new_alpha_H = RKRand.rk_gamma(alpha_h_d + Ls + s - 1, alpha_h_f - Math.log(q));

        return new_alpha_H;
    }

    public double[] alphaUpdate() {
        double[] new_alpha = new double[D];

        for (int d = 0; d < D; ++d) {
            int docoff = d * L;
            int Is = 0;
            for (int l = 0; l < L; ++l) {
                if (dishByRestaurantCounts[docoff + l] > 0) {
                    Is++;
                }
            }

            double q = RKRand.rk_beta(alpha[d] + 1, Is);
            int s = rand.nextDouble() < q ? 1 : 0;
            new_alpha[d] = RKRand.rk_gamma(alpha_shape_a + Is - s, alpha_scale_b - Math.log(q));
        }

        return new_alpha;
    }

    public static double[] dirichletUpdate(double[] _c0, int[] _n) {
        double[] hyp = new double[_n.length];
        for (int i = 0; i < _n.length; ++i) {
            hyp[i] = _c0[i] + _n[i];
        }
        double[] phi = TGRand.dirichletRnd(hyp);
        return phi;
    }

    public double[] dirichletUpdate(double _c0, int[] _n) {
        double[] hyp = new double[_n.length];
        for (int i = 0; i < _n.length; ++i) {
            hyp[i] = _c0 + _n[i];
        }
        double[] phi = TGRand.dirichletRnd(hyp);
        return phi;
    }

    public double[][] regionMeansUpdate() {
        double[][] newRegionMeans = new double[L][];

        for (int l = 0; l < L; ++l) {
            double[] oldmean = regionMeans[l];

            double[] newmean = TGRand.vmfRnd(oldmean, vmf_proposal_kappa);
            double u = evalMuLogLikelihood(newmean, l) - evalMuLogLikelihood(oldmean, l);

            if (u > 0) {
                newRegionMeans[l] = newmean;
            } else {
                if (rand.nextDouble() < Math.exp(u)) {
                    newRegionMeans[l] = newmean;
                } else {
                    newRegionMeans[l] = oldmean;
                }
            }
        }
        return newRegionMeans;
    }

    public double[] kappaUpdate() {
        double[] newkappa = new double[L];

        for (int l = 0; l < L; ++l) {
            double oldk = kappa[l];
            double newk = vmf_proposal_sigma * rand.nextGaussian() + oldk;
            while (newk < 0) {
                newk = vmf_proposal_sigma * rand.nextGaussian() + oldk;
            }
            double[] means = regionMeans[l];

            double u = evalKappaLogLikelihood(newk, means, l) - evalKappaLogLikelihood(oldk, means, l);
            if (u > 0) {
                newkappa[l] = newk;
            } else {
                if (rand.nextDouble() < Math.exp(u)) {
                    newkappa[l] = newk;
                } else {
                    newkappa[l] = oldk;
                }
            }
        }
        return newkappa;
    }

    public double[] restaurantStickBreakingWeightsUpdate() {
        double[] weights = new double[D * L];
        double[] wcs = TGMath.cumSum(globalDishWeights);
        for (int d = 0; d < D; ++d) {
            int docoff = d * L;
            double ai = alpha[d];

            double[] vl = new double[L];
            double[] ivl = new double[L];
            double[] ilvl = new double[L];
            int[] incs = TGMath.inverseCumSum(dishByRestaurantCounts, docoff, docoff + L);

            for (int l = 0; l < L - 1; ++l) {
                double a = ai * globalDishWeights[l] + dishByRestaurantCounts[docoff + l];
                double b = ai * (1 - wcs[l]) + incs[l + 1];
                vl[l] = RKRand.rk_beta(a, b);
            }

            vl[L] = 1;
            for (int i = 0; i < L; ++i) {
                ilvl[i] = Math.log(1 - vl[i]);
            }
            ivl = TGMath.cumSum(ilvl);

            weights[docoff] = vl[0];
            for (int l = 1; l < L; ++l) {
                weights[docoff + l] = Math.exp(Math.log(vl[l]) + ivl[l - 1]);
            }
        }
        return weights;
    }

    public double[] globalStickBreakingWeightsUpdate() {
        double[] weights = new double[L];
        double[] v = new double[L];
        double[] ivl = new double[L];
        double[] ilvl = new double[L];
        int[] incs = TGMath.inverseCumSum(globalDishCounts);

        for (int i = 0; i < L - 1; ++i) {
            double a = 1 + globalDishCounts[i];
            double b = alpha_H + incs[i + 1];
            v[i] = RKRand.rk_beta(a, b);
        }

        v[L] = 1;
        for (int i = 0; i < L; ++i) {
            ilvl[i] = Math.log(1 - v[i]);
        }
        ivl = TGMath.cumSum(ilvl);

        weights[0] = v[0];
        for (int i = 1; i < L; ++i) {
            weights[i] = Math.exp(Math.log(v[i]) + ivl[i - 1]);
        }

        return weights;
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
