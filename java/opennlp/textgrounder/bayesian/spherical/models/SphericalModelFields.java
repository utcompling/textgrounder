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
     * Counts of topics
     */
    protected int[] regionCounts;
    /**
     * 
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
    protected double[] normalizedWordByRegionCounts;
    /**
     *
     */
    protected double[] normalizedRegionByDocumentCounts;
    /**
     * 
     */
    protected double[] normalizedAllWordsRegionCounts;
    /**
     *
     */
    protected double[][] normalizedRegionMeans;
    /**
     * 
     */
    protected double[][][] normalizedRegionToponymCoordinateCounts;

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
        return expectedR;
    }

    public int getW() {
        return W;
    }

    public void setW(int W) {
        this.W = W;
    }
}
