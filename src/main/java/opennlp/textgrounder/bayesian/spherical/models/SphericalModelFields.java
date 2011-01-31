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
     * 
     */
    protected double[] kappa;
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
    protected int Nn;
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
    protected int[] regionVector;
    /**
     *
     */
    protected int[] topicVector;
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
     * Vector of word indices
     */
    protected int[] wordVector;
    /**
     * Counts of topics per document
     */
    protected int[] regionByDocumentCounts;
    /**
     * only used if there is an independent topic model component to model
     */
    protected int[] dishByRestaurantCounts;
    /**
     * 
     */
    protected int[][] toponymCoordinateCounts;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByRegionCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected double[][][] toponymCoordinateLexicon;
    /**
     * 
     */
    protected double[][] regionMeans;
    /**
     * Counts of regions but only for toponyms
     */
    protected int[] dishCountsOfToponyms;
    /**
     *
     */
    protected int[] nonToponymByDishCounts;
    /**
     *
     */
    protected int[] globalDishCounts;
    /**
     *
     */
    protected double[] globalDishWeights;
    /**
     *
     */
    protected double[] localDishWeights;
    /**
     * Counts of regions for all words
     */
    protected int[] regionCountsOfAllWords;
    /**
     *
     */
    protected double[] nonToponymByDishDirichlet;
    /**
     *
     */
    protected double[][] toponymCoordinateWeights;
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

    public int[] getAllWordsRegionCounts() {
        return regionCountsOfAllWords;
    }

    public void setAllWordsRegionCounts(int[] allWordsRegionCounts) {
        this.regionCountsOfAllWords = allWordsRegionCounts;
    }

    public double getAlpha() {
        return alpha_H;
    }

    public void setAlpha(double alpha) {
        this.alpha_H = alpha;
    }

    public double[] getAveragedAllWordsRegionCounts() {
        return localDishWeightsFM;
    }

    public void setAveragedAllWordsRegionCounts(double[] averagedAllWordsRegionCounts) {
        this.localDishWeightsFM = averagedAllWordsRegionCounts;
    }

    public double[] getAveragedRegionByDocumentCounts() {
        return kappaFM;
    }

    public void setAveragedRegionByDocumentCounts(double[] averagedRegionByDocumentCounts) {
        this.kappaFM = averagedRegionByDocumentCounts;
    }

    public double[][] getAveragedRegionMeans() {
        return regionMeansFM;
    }

    public void setAveragedRegionMeans(double[][] averagedRegionMeans) {
        this.regionMeansFM = averagedRegionMeans;
    }

    public double[][] getAveragedRegionToponymCoordinateCounts() {
        return toponymCoordinateDirichletFM;
    }

    public void setAveragedRegionToponymCoordinateCounts(double[][] averagedRegionToponymCoordinateCounts) {
        this.toponymCoordinateDirichletFM = averagedRegionToponymCoordinateCounts;
    }

    public double[] getAveragedWordByRegionCounts() {
        return globalDishWeightsFM;
    }

    public void setAveragedWordByRegionCounts(double[] averagedWordByRegionCounts) {
        this.globalDishWeightsFM = averagedWordByRegionCounts;
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

    public int getL() {
        return L;
    }

    public void setL(int L) {
        this.L = L;
    }

    public int getMaxCoord() {
        return maxCoord;
    }

    public void setMaxCoord(int maxCoord) {
        this.maxCoord = maxCoord;
    }

    public int[] getRegionByDocumentCounts() {
        return regionByDocumentCounts;
    }

    public void setRegionByDocumentCounts(int[] regionByDocumentCounts) {
        this.regionByDocumentCounts = regionByDocumentCounts;
    }

    public int[] getRegionCounts() {
        return dishCountsOfToponyms;
    }

    public void setRegionCounts(int[] regionCounts) {
        this.dishCountsOfToponyms = regionCounts;
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

    public int[] getRegionVector() {
        return regionVector;
    }

    public void setRegionVector(int[] regionVector) {
        this.regionVector = regionVector;
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

    public double[] getAveragedTopicByDocumentCounts() {
        return nonToponymByDishDirichletFM;
    }

    public void setAveragedTopicByDocumentCounts(double[] averagedTopicByDocumentCounts) {
        this.nonToponymByDishDirichletFM = averagedTopicByDocumentCounts;
    }

    public int[] getTopicByDocumentCounts() {
        return dishByRestaurantCounts;
    }

    public void setTopicByDocumentCounts(int[] topicByDocumentCounts) {
        this.dishByRestaurantCounts = topicByDocumentCounts;
    }

    public int[] getToponymRegionCounts() {
        return dishCountsOfToponyms;
    }

    public void setToponymRegionCounts(int[] toponymRegionCounts) {
        this.dishCountsOfToponyms = toponymRegionCounts;
    }
}
