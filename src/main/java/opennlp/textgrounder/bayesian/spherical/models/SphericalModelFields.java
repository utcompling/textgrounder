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

import java.io.Serializable;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalModelFields implements Serializable {

    private static final long serialVersionUID = 42L;

    /**
     * global stick breaking prior. is specified from param file on first
     * iteration.
     */
    protected double alpha_H;
    /**
     * fixed intermediate parameters for resampling alpha_H. corresponds to
     * d and f in paper
     */
    protected double alpha_h_d, alpha_h_f;
    /**
     * uniform hyperparameter for restaurant local stick breaking weights. only
     * used in first iteration. set from param file
     */
    protected double alpha_init;
    /**
     * restaurant local stick breaking prior
     */
    protected double[] alpha;
    /**
     * hyperparameters for sampling document specific alpha parameters in posterior
     * update stage. corresponds to a and b respectively in algorithm paper
     */
    protected double alpha_shape_a = 0.1;
    protected double alpha_scale_b = 0.1;
    /**
     * gamma hyperparameters for kappa generation in metropolis-hastings
     */
    protected double kappa_hyper_shape = 1, kappa_hyper_scale = 0.01;
    /**
     * dirichlet hyperparameter for non-toponym word by dish weights. corresponding
     * random variable is labeled phi in the algorithm paper. corresponding hyperparameter
     * is c_0 in paper.
     */
    protected double phi_dirichlet_hyper = 0.1;
    /**
     * dirichlet hyperparameter for coordinate candidates of toponyms. corresponding
     * random variable is ehta in the algorithm paper. corresponding hyperparameter
     * is d_0 in paper.
     */
    protected double ehta_dirichlet_hyper = 0.1;
    /**
     * the concentration parameter in the proposal distribution for region means
     */
    protected double vmf_proposal_kappa = 50;
    /**
     * the standard deviation in the proposal distribution for region kappas
     */
    protected double vmf_proposal_sigma = 1;
    /**
     * Number of documents
     */
    protected int D;
    /**
     * Number of tokens
     */
    protected int N;
    /**
     * Number of tokens minus stopwords. Or total number of valid customers
     * across restaurants
     */
    protected int nonStopwordN;
    /**
     * Expected maximum number of dishes in the restaurants
     */
    protected int L;
    /**
     * Number of non-stopword word types.
     */
    protected int W;
    /**
     * Number of toponyms
     */
    protected int T;
    /**
     *
     */
    protected int maxCoord;
    /**
     * cartesian coordinates is the default. see no reason to move to slower (but cheaper)
     * spherical coordinates
     */
    protected int coordParamLen = 3;
    /**
     * Vector of topics
     */
    protected int[] dishVector;
    /**
     * Vector of document indices
     */
    protected int[] documentVector;
    /**
     * Vector of stopwords. If 0, the word is not a stopword. If 1, it is.
     */
    protected int[] stopwordVector;
    /**
     *
     */
    protected int[] coordinateVector;
    /**
     * Vector of toponyms. If 0, the word is not a toponym. If 1, it is.
     */
    protected int[] toponymVector;
    /**
     * Vector of toponyms offsets. It's a fast index to only the offsets of
     * toponyms in wordVector 
     */
    protected int[] toponymIdxVector;
    /**
     * Vector of word indices
     */
    protected int[] wordVector;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByRegionCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected double[][][] toponymCoordinateLexicon;
    ///////////////////////////////////////////////////////////////////////////
    /**
     * Count arrays
     */
    ///////////////////////////////////////////////////////////////////////////
    /**
     * only used if there is an independent topic model component to model
     */
    protected int[] dishByRestaurantCounts;
    /**
     * 
     */
    protected int[][] toponymCoordinateCounts;
    /**
     * Counts of regions but only for toponyms
     */
    protected int[] toponymByDishCounts;
    /**
     *
     */
    protected int[] nonToponymByDishCounts;
    /**
     *
     */
    protected int[] globalDishCounts;
    ///////////////////////////////////////////////////////////////////////////
    /**
     * Weights/parameters at each iteration
     */
    ///////////////////////////////////////////////////////////////////////////
    /**
     *
     */
    protected double[][] regionMeans;
    /**
     *
     */
    protected double[] kappa;
    /**
     *
     */
    protected double[] globalDishWeights;
    /**
     *
     */
    protected double[] localDishWeights;
    /**
     *
     */
    protected double[] nonToponymByDishDirichlet;
    /**
     *
     */
    protected double[][] toponymCoordinateDirichlet;
    ///////////////////////////////////////////////////////////////////////////
    /**
     * Sample averages. Sample first moments and second moments
     */
    ///////////////////////////////////////////////////////////////////////////
    /**
     *
     */
    protected double[] globalDishWeightsFM;
    /**
     *
     */
    protected double[] localDishWeightsFM;
    /**
     *
     */
    protected double[] kappaFM;
    /**
     * 
     */
    protected double[] nonToponymByDishDirichletFM;
    /**
     *
     */
    protected double[][] regionMeansFM;
    /**
     * 
     */
    protected double[][] toponymCoordinateDirichletFM;

    public int getD() {
        return D;
    }

    public void setD(int D) {
        this.D = D;
    }

    public int getN() {
        return N;
    }

    public void setN(int N) {
        this.N = N;
    }

    public int getW() {
        return W;
    }

    public void setW(int W) {
        this.W = W;
    }

    public int getT() {
        return T;
    }

    public void setT(int T) {
        this.T = T;
    }

    public int getL() {
        return L;
    }

    public void setL(int L) {
        this.L = L;
    }

    public double getAlpha() {
        return alpha_H;
    }

    public void setAlpha(double alpha) {
        this.alpha_H = alpha;
    }

    public double[] getLocalDishWeightsFM() {
        return localDishWeightsFM;
    }

    public void setLocalDishWeightsFM(double[] _localDishWeightsFM) {
        this.localDishWeightsFM = _localDishWeightsFM;
    }

    public double[] getKappaFM() {
        return kappaFM;
    }

    public void setKappaFM(double[] _kappaFM) {
        this.kappaFM = _kappaFM;
    }

    public double[][] getRegionMeansFM() {
        return regionMeansFM;
    }

    public void setRegionMeansFM(double[][] _regionMeansFM) {
        this.regionMeansFM = _regionMeansFM;
    }

    public double[][] getToponymCoordinateDirichletFM() {
        return toponymCoordinateDirichletFM;
    }

    public void setToponymCoordinateDirichletFM(double[][] _toponymCoordinateDirichletFM) {
        this.toponymCoordinateDirichletFM = _toponymCoordinateDirichletFM;
    }

    public double[] getGlobalDishWeightsFM() {
        return globalDishWeightsFM;
    }

    public void setGlobalDishWeightsFM(double[] _globalDishWeightsFM) {
        this.globalDishWeightsFM = _globalDishWeightsFM;
    }

    public int getCoordParamLen() {
        return coordParamLen;
    }

    public void setCoordParamLen(int coordParamLen) {
        this.coordParamLen = coordParamLen;
    }

    public int[] getCoordinateVector() {
        return coordinateVector;
    }

    public void setCoordinateVector(int[] coordinateVector) {
        this.coordinateVector = coordinateVector;
    }

    public int[] getDocumentVector() {
        return documentVector;
    }

    public void setDocumentVector(int[] documentVector) {
        this.documentVector = documentVector;
    }

    public int getMaxCoord() {
        return maxCoord;
    }

    public void setMaxCoord(int maxCoord) {
        this.maxCoord = maxCoord;
    }

    public int[] getRegionCounts() {
        return toponymByDishCounts;
    }

    public void setRegionCounts(int[] regionCounts) {
        this.toponymByDishCounts = regionCounts;
    }

    public double[][] getRegionMeans() {
        return regionMeans;
    }

    public void setRegionMeans(double[][] regionMeans) {
        this.regionMeans = regionMeans;
    }

    public int[][] getRegionToponymCoordinateCounts() {
        return toponymCoordinateCounts;
    }

    public void setRegionToponymCoordinateCounts(int[][] regionToponymCoordinateCounts) {
        this.toponymCoordinateCounts = regionToponymCoordinateCounts;
    }

    public int[] getDishVector() {
        return dishVector;
    }

    public void setDishVector(int[] _dishVector) {
        this.dishVector = _dishVector;
    }

    public int[] getStopwordVector() {
        return stopwordVector;
    }

    public void setStopwordVector(int[] stopwordVector) {
        this.stopwordVector = stopwordVector;
    }

    public double[][][] getToponymCoordinateLexicon() {
        return toponymCoordinateLexicon;
    }

    public void setToponymCoordinateLexicon(double[][][] toponymCoordinateLexicon) {
        this.toponymCoordinateLexicon = toponymCoordinateLexicon;
    }

    public int[] getToponymVector() {
        return toponymVector;
    }

    public void setToponymVector(int[] toponymVector) {
        this.toponymVector = toponymVector;
    }

    public int[] getWordVector() {
        return wordVector;
    }

    public void setWordVector(int[] wordVector) {
        this.wordVector = wordVector;
    }

    public double[] getNonToponymByDishDirichletFM() {
        return nonToponymByDishDirichletFM;
    }

    public void setNonToponymByDishDirichletFM(double[] _nonToponymByDishDirichletFM) {
        this.nonToponymByDishDirichletFM = _nonToponymByDishDirichletFM;
    }

    public int[] getDishByRestaurantCounts() {
        return dishByRestaurantCounts;
    }

    public void setDishByRestaurantCounts(int[] _dishByRestaurantCounts) {
        this.dishByRestaurantCounts = _dishByRestaurantCounts;
    }

    public int[] getToponymByDishCounts() {
        return toponymByDishCounts;
    }

    public void setToponymByDishCounts(int[] _toponymByDishCounts) {
        this.toponymByDishCounts = _toponymByDishCounts;
    }
}
