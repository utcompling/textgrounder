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

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalModelFields {

    /**
     * Hyperparameter for CRP
     */
    protected double crpalpha;
    /**
     * Hyperparameter for region*doc priors
     */
    protected double alpha;
    /**
     * Hyperparameter for word*region priors
     */
    protected double beta;
    /**
     * Normalization term for word*region gibbs sampler
     */
    protected double betaW;
    /**
     * 
     */
    protected double kappa;
    /**
     * Number of documents
     */
    protected int D;
    /**
     * Number of tokens
     */
    protected int N;
    /**
     * Expected maximum number of regions/tables in the chinese restaurant
     */
    protected int expectedR;
    /**
     * Current number of regions/tables in the chinese restaurant
     */
    protected int currentR;
    /**
     *
     */
    protected int emptyR;
    /**
     *
     */
    protected int maxCoord;
    /**
     * Number of non-stopword word types.
     */
    protected int W;
    /**
     * Number of toponyms
     */
    protected int T;
    /**
     * number of non-toponym topics
     */
    protected int Z;
    /**
     * cartesian coordinates is the default. see no reason to move to slower (but cheaper)
     * spherical coordinates
     */
    protected int coordParamLen = 3;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByRegionCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected double[][][] toponymCoordinateLexicon;
    /**
     * Vector of document indices
     */
    protected int[] documentVector;
    /**
     * Counts of topics per document
     */
    protected int[] regionByDocumentCounts;
    /**
     * 
     */
//    protected int[] regionCoordinateCounts;
    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected int[] wordByRegionCounts;
    /**
     * 
     */
//    protected int[] toponymByRegionCounts;
    /**
     * 
     */
    protected int[][][] regionToponymCoordinateCounts;
    /**
     * 
     */
    protected double[][] regionMeans;
    /**
     * Counts of regions but only for toponyms
     */
    protected int[] toponymRegionCounts;
    /**
     * Counts of regions for all words
     */
    protected int[] allWordsRegionCounts;
    /**
     * Vector of topics
     */
    protected int[] regionVector;
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
     *
     */
//    protected double[] normalizedRegionCounts;
    /**
     *
     */
    protected double[] averagedWordByRegionCounts;
    /**
     *
     */
    protected double[] averagedAllWordsRegionCounts;
    /**
     *
     */
    protected double[] averagedRegionByDocumentCounts;
    /**
     *
     */
    protected double[][] averagedRegionMeans;
    /**
     * 
     */
    protected double[][][] averagedRegionToponymCoordinateCounts;

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
        return allWordsRegionCounts;
    }

    public void setAllWordsRegionCounts(int[] allWordsRegionCounts) {
        this.allWordsRegionCounts = allWordsRegionCounts;
    }

    public double getAlpha() {
        return alpha;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public double[] getAveragedAllWordsRegionCounts() {
        return averagedAllWordsRegionCounts;
    }

    public void setAveragedAllWordsRegionCounts(double[] averagedAllWordsRegionCounts) {
        this.averagedAllWordsRegionCounts = averagedAllWordsRegionCounts;
    }

    public double[] getAveragedRegionByDocumentCounts() {
        return averagedRegionByDocumentCounts;
    }

    public void setAveragedRegionByDocumentCounts(double[] averagedRegionByDocumentCounts) {
        this.averagedRegionByDocumentCounts = averagedRegionByDocumentCounts;
    }

    public double[][] getAveragedRegionMeans() {
        return averagedRegionMeans;
    }

    public void setAveragedRegionMeans(double[][] averagedRegionMeans) {
        this.averagedRegionMeans = averagedRegionMeans;
    }

    public double[][][] getAveragedRegionToponymCoordinateCounts() {
        return averagedRegionToponymCoordinateCounts;
    }

    public void setAveragedRegionToponymCoordinateCounts(double[][][] averagedRegionToponymCoordinateCounts) {
        this.averagedRegionToponymCoordinateCounts = averagedRegionToponymCoordinateCounts;
    }

    public double[] getAveragedWordByRegionCounts() {
        return averagedWordByRegionCounts;
    }

    public void setAveragedWordByRegionCounts(double[] averagedWordByRegionCounts) {
        this.averagedWordByRegionCounts = averagedWordByRegionCounts;
    }

    public double getBeta() {
        return beta;
    }

    public void setBeta(double beta) {
        this.beta = beta;
    }

    public double getBetaW() {
        return betaW;
    }

    public void setBetaW(double betaW) {
        this.betaW = betaW;
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

    public double getCrpalpha() {
        return crpalpha;
    }

    public void setCrpalpha(double crpalpha) {
        this.crpalpha = crpalpha;
    }

    public int getCurrentR() {
        return currentR;
    }

    public void setCurrentR(int currentR) {
        this.currentR = currentR;
    }

    public int[] getDocumentVector() {
        return documentVector;
    }

    public void setDocumentVector(int[] documentVector) {
        this.documentVector = documentVector;
    }

    public int getEmptyR() {
        return emptyR;
    }

    public void setEmptyR(int emptyR) {
        this.emptyR = emptyR;
    }

    public int getExpectedR() {
        return expectedR;
    }

    public void setExpectedR(int expectedR) {
        this.expectedR = expectedR;
    }

    public double getKappa() {
        return kappa;
    }

    public void setKappa(double kappa) {
        this.kappa = kappa;
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
        return toponymRegionCounts;
    }

    public void setRegionCounts(int[] regionCounts) {
        this.toponymRegionCounts = regionCounts;
    }

    public double[][] getRegionMeans() {
        return regionMeans;
    }

    public void setRegionMeans(double[][] regionMeans) {
        this.regionMeans = regionMeans;
    }

    public int[][][] getRegionToponymCoordinateCounts() {
        return regionToponymCoordinateCounts;
    }

    public void setRegionToponymCoordinateCounts(int[][][] regionToponymCoordinateCounts) {
        this.regionToponymCoordinateCounts = regionToponymCoordinateCounts;
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

    public int[] getWordByRegionCounts() {
        return wordByRegionCounts;
    }

    public void setWordByRegionCounts(int[] wordByRegionCounts) {
        this.wordByRegionCounts = wordByRegionCounts;
    }

    public int[] getWordVector() {
        return wordVector;
    }

    public void setWordVector(int[] wordVector) {
        this.wordVector = wordVector;
    }
}
