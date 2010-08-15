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
package opennlp.textgrounder.bayesian.rlda.models;

import java.io.Serializable;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class RegionModelFields implements Serializable {

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
     * Number of documents
     */
    protected int D;
    /**
     * Number of tokens
     */
    protected int N;
    /**
     * Number of regions
     */
    protected int R;
    /**
     * Number of non-stopword word types. Equivalent to <p>fW-sW</p>.
     */
    protected int W;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByRegionCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected int[] regionByToponymFilter;
    /**
     *
     */
    protected int[] activeRegionByDocumentFilter;
    /**
     * Vector of document indices
     */
    protected int[] documentVector;
    /**
     * Counts of topics per document
     */
    protected int[] regionByDocumentCounts;
    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected int[] wordByRegionCounts;
    /**
     * Counts of topics
     */
    protected int[] regionCounts;
    /**
     * Vector of topics
     */
    protected int[] regionVector;
    /**
     * Vector of stopwords. If 0, the word is not a stopword. If 1, it is.
     */
    protected int[] stopwordVector;
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
    protected double[] normalizedRegionCounts;
    /**
     * 
     */
    protected double[] normalizedWordByRegionCounts;
    /**
     * 
     */
    protected double[] normalizedRegionByDocumentCounts;
    /**
     * Posterior probabilities for topics.
     */
    protected double[] regionProbs;
    /**
     * Probability of word given topic. since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected double[] wordByRegionProbs;
    /**
     *
     */
    protected double[] regionByDocumentProbs;

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

    public int getR() {
        return R;
    }

    public void setR(int R) {
        this.R = R;
    }

    public int getW() {
        return W;
    }

    public void setW(int W) {
        this.W = W;
    }

    public int[] getActiveRegionByDocumentFilter() {
        return activeRegionByDocumentFilter;
    }

    public void setActiveRegionByDocumentFilter(
          int[] activeRegionByDocumentFilter) {
        this.activeRegionByDocumentFilter = activeRegionByDocumentFilter;
    }

    public double getAlpha() {
        return alpha;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
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

    public int[] getDocumentVector() {
        return documentVector;
    }

    public void setDocumentVector(int[] documentVector) {
        this.documentVector = documentVector;
    }

    public int[] getRegionByDocumentCounts() {
        return regionByDocumentCounts;
    }

    public void setRegionByDocumentCounts(int[] regionByDocumentCounts) {
        this.regionByDocumentCounts = regionByDocumentCounts;
    }

    public int[] getRegionByToponymFilter() {
        return regionByToponymFilter;
    }

    public void setRegionByToponymFilter(int[] regionByToponymFilter) {
        this.regionByToponymFilter = regionByToponymFilter;
    }

    public int[] getRegionCounts() {
        return regionCounts;
    }

    public void setRegionCounts(int[] regionCounts) {
        this.regionCounts = regionCounts;
    }

    public double[] getRegionProbs() {
        return normalizedRegionCounts;
    }

    public void setRegionProbs(double[] regionProbs) {
        this.normalizedRegionCounts = regionProbs;
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

    public double[] getWordByRegionProbs() {
        return normalizedWordByRegionCounts;
    }

    public void setWordByRegionProbs(double[] wordByRegionProbs) {
        this.normalizedWordByRegionCounts = wordByRegionProbs;
    }

    public int[] getWordVector() {
        return wordVector;
    }

    public void setWordVector(int[] wordVector) {
        this.wordVector = wordVector;
    }

    public double[] getNormalizedRegionByDocumentCounts() {
        return normalizedRegionByDocumentCounts;
    }

    public void setNormalizedRegionByDocumentCounts(double[] normalizedRegionByDocumentCounts) {
        this.normalizedRegionByDocumentCounts = normalizedRegionByDocumentCounts;
    }

    public double[] getNormalizedRegionCounts() {
        return normalizedRegionCounts;
    }

    public void setNormalizedRegionCounts(double[] normalizedRegionCounts) {
        this.normalizedRegionCounts = normalizedRegionCounts;
    }

    public double[] getNormalizedWordByRegionCounts() {
        return normalizedWordByRegionCounts;
    }

    public void setNormalizedWordByRegionCounts(double[] normalizedWordByRegionCounts) {
        this.normalizedWordByRegionCounts = normalizedWordByRegionCounts;
    }
}
