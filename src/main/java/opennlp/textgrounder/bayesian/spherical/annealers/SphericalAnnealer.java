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
package opennlp.textgrounder.bayesian.spherical.annealers;

import java.util.Arrays;
import opennlp.textgrounder.bayesian.annealers.Annealer;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.mathutils.TGBLAS;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class SphericalAnnealer extends Annealer {

    /**
     * 
     */
    protected int geoMeanVecLen;
    /**
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected double[] wordByRegionFirstMoment;
    /**
     *
     */
    protected double[] allWordsRegionFirstMoment;
    /**
     * 
     */
    protected double[] regionByDocumentFirstMoment;
    /**
     * 
     */
    protected double[] topicByDocumentFirstMoment;
    /**
     *
     */
    protected double[][] regionMeansFirstMoment;
    /**
     *
     */
    protected double[][] regionMeansSecondMoment;
    /**
     *
     */
    protected double[][] toponymCoordinateFirstMoment;
    /**
     *
     */
    protected double[][] toponymCoordinateSecondMoment;

    /**
     *
     */
//    protected double[] nonToponymRegionCounts;
    protected SphericalAnnealer() {
    }

    public SphericalAnnealer(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
    }

    public abstract double annealProbs(int _starti, int _endi, double[] _classes);

    public abstract double annealProbs(int _R, int _subC, int _C, double[] _classes);

    /**
     *
     * @param _wordByRegionCounts
     * @param _regionByDocumentCounts
     * @param _allWordsRegionCounts
     * @param _regionMeans
     * @param _regionToponymCoordinateCounts
     */
    protected void initializeCollectionArrays(int[] _wordByRegionCounts, int[] _regionByDocumentCounts,
          int[] _allWordsRegionCounts, double[][] _regionMeans,
          int[][][] _toponymCoordinateWeights) {
        wordByRegionFirstMoment = new double[_wordByRegionCounts.length];
        regionByDocumentFirstMoment = new double[_regionByDocumentCounts.length];
        allWordsRegionFirstMoment = new double[_allWordsRegionCounts.length];

        Arrays.fill(wordByRegionFirstMoment, 0);
        Arrays.fill(regionByDocumentFirstMoment, 0);
        Arrays.fill(allWordsRegionFirstMoment, 0);

        toponymCoordinateFirstMoment = new double[_toponymCoordinateWeights.length][];
        toponymCoordinateSecondMoment = new double[_toponymCoordinateWeights.length][];
        for (int i = 0; i < _toponymCoordinateWeights.length; ++i) {
            toponymCoordinateFirstMoment[i] = new double[_toponymCoordinateWeights[i].length];
            Arrays.fill(toponymCoordinateFirstMoment[i], 0);
            toponymCoordinateSecondMoment[i] = new double[_toponymCoordinateWeights[i].length];
            Arrays.fill(toponymCoordinateSecondMoment[i], 0);
        }

        regionMeansFirstMoment = new double[_regionMeans.length][];
        geoMeanVecLen = _regionMeans[0].length;
        for (int i = 0; i < _regionMeans.length; ++i) {
            double[] mean = new double[geoMeanVecLen];
            for (int j = 0; j < geoMeanVecLen; ++j) {
                mean[j] = 0;
            }
            regionMeansFirstMoment[i] = mean;
        }
    }

    protected void addToArrays(int[] _wordByRegionCounts, int[] _regionByDocumentCounts,
          int[] _allWordsRegionCounts, double[][] _regionMeans,
          int[][][] _regionToponymCoordinateCounts) {
        addToArray(wordByRegionFirstMoment, _wordByRegionCounts);
        addToArray(regionByDocumentFirstMoment, _regionByDocumentCounts);
        addToArray(allWordsRegionFirstMoment, _allWordsRegionCounts);

//        for (int i = 0; i < toponymCoordinateWeights.length; ++i) {
//            for (int j = 0; j < toponymCoordinateWeights[i].length; ++j) {
//                for (int k = 0; k < toponymCoordinateWeights[i][j].length; ++k) {
//                    toponymCoordinateWeights[i][j][k] += _regionToponymCoordinateCounts[i][j][k];
//                }
//            }
//        }

        for (int i = 0; i < regionMeansFirstMoment.length; ++i) {
            TGBLAS.daxpy(geoMeanVecLen, 1, _regionMeans[i], 1, regionMeansFirstMoment[i], 1);
        }
    }

    /**
     * 
     * @param _wordByRegionCounts
     * @param _regionByDocumentCounts
     * @param _allWordsRegionCounts
     * @param _regionMeans
     * @param _regionToponymCoordinateCounts
     */
    public void collectSamples(int[] _wordByRegionCounts, int[] _regionByDocumentCounts,
          int[] _allWordsRegionCounts, double[][] _regionMeans,
          //          int[] _toponymByRegionCounts, int[] _nonToponymRegionCounts,
          int[][][] _regionToponymCoordinateCounts) {

        if (sampleCount < samples) {
            if (sampleiteration && (innerIter % lag == 0)) {
                sampleCount += 1;
                if (samples == sampleCount) {
                    finishedCollection = true;
                }

                System.err.print("(sample:" + (innerIter + 1) / lag + ")");
                if (wordByRegionFirstMoment == null) {
                    initializeCollectionArrays(_wordByRegionCounts, _regionByDocumentCounts, _allWordsRegionCounts, _regionMeans, _regionToponymCoordinateCounts);
                }
                addToArrays(_wordByRegionCounts, _regionByDocumentCounts, _allWordsRegionCounts, _regionMeans, _regionToponymCoordinateCounts);
            }
            if (finishedCollection) {
                averageSamples();
            }
        }
    }

    public void collectSamples(int[] _wordByRegionCounts, int[] _regionByDocumentCounts,
          int[] _topicByDocumentCounts, int[] _allWordsRegionCounts, double[][] _regionMeans,
          int[][][] _regionToponymCoordinateCounts) {

        if (sampleCount < samples) {
            if (sampleiteration && (innerIter % lag == 0)) {
                sampleCount += 1;
                if (samples == sampleCount) {
                    finishedCollection = true;
                }

                System.err.print("(sample:" + (innerIter + 1) / lag + ")");
                if (wordByRegionFirstMoment == null) {
                    initializeCollectionArrays(_wordByRegionCounts, _regionByDocumentCounts, _allWordsRegionCounts, _regionMeans, _regionToponymCoordinateCounts);

                    topicByDocumentFirstMoment = new double[_topicByDocumentCounts.length];
                    Arrays.fill(topicByDocumentFirstMoment, 0);
                }

                addToArrays(_wordByRegionCounts, _regionByDocumentCounts, _allWordsRegionCounts, _regionMeans, _regionToponymCoordinateCounts);
                addToArray(topicByDocumentFirstMoment, _topicByDocumentCounts);
            }
            if (finishedCollection) {
                averageSamples();
            }
        }
    }

    protected void averageSamples() {
        averageSamples(wordByRegionFirstMoment);
        averageSamples(regionByDocumentFirstMoment);
        averageSamples(allWordsRegionFirstMoment);
        try {
            averageSamples(topicByDocumentFirstMoment);
        } catch (NullPointerException e) {
        }

        for (int i = 0; i < toponymCoordinateFirstMoment.length;
              ++i) {
            for (int j = 0;
                  j < toponymCoordinateFirstMoment[i].length; ++j) {
//                for (int k = 0;
//                      k < toponymCoordinateWeights[i][j].length;
//                      ++k) {
//                    toponymCoordinateWeights[i][j][k] /= sampleCount;
//                }
            }
        }

        for (int i = 0; i < regionMeansFirstMoment.length; ++i) {
            for (int j = 0; j < geoMeanVecLen; ++j) {
                regionMeansFirstMoment[i][j] /= sampleCount;
            }
        }
    }

    public double[] getRegionByDocumentCounts() {
        return regionByDocumentFirstMoment;
    }

    public double[][] getRegionToponymCoordinateCounts() {
        return toponymCoordinateFirstMoment;
    }

    public double[] getWordByRegionCounts() {
        return wordByRegionFirstMoment;
    }

    public void setRegionByDocumentCounts(double[] _regionByDocumentCounts) {
        regionByDocumentFirstMoment = _regionByDocumentCounts;
    }

    public void setRegionToponymCoordinateCounts(double[][] _regionToponymCoordinateCounts) {
        toponymCoordinateFirstMoment = _regionToponymCoordinateCounts;
    }

    public void setWordByRegionCounts(double[] _wordByRegionCounts) {
        wordByRegionFirstMoment = _wordByRegionCounts;
    }

    public double[] getAllWordsRegionCounts() {
        return allWordsRegionFirstMoment;
    }

    public void setAllWordsRegionCounts(double[] _allWordsRegionCounts) {
        allWordsRegionFirstMoment = _allWordsRegionCounts;
    }

    public double[][] getRegionMeans() {
        return regionMeansFirstMoment;
    }

    public void setRegionMeans(double[][] _regionMeans) {
        regionMeansFirstMoment = _regionMeans;
    }

    public double[] getTopicByDocumentCounts() {
        return topicByDocumentFirstMoment;
    }

    public void setTopicByDocumentCounts(double[] topicByDocumentCounts) {
        this.topicByDocumentFirstMoment = topicByDocumentCounts;
    }
}
