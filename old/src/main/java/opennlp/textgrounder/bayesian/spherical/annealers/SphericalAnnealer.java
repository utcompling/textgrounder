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
     * collection of first moments
     */
    protected double[] globalDishWeightsFM;
    protected double[] localDishWeightsFM;
    protected double[][] regionMeansFM;
    protected double[] kappaFM;
    protected double[] nonToponymByDishDirichletFM;
    protected double[][] toponymCoordinateDirichletFM;
    /**
     * collection of second moments
     */
    protected double[] globalDishWeightsSM;
    protected double[] localDishWeightsSM;
//    protected double[][] regionMeansSM;
    protected double[] kappaSM;
    protected double[] nonToponymByDishDirichletSM;
    protected double[][] toponymCoordinateDirichletSM;
    /**
     * 
     */
    protected int geoMeanVecLen;
    protected int geoCovVecLen;

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
    protected void initializeCollectionArrays(double[] _globalDishWeights, double[] _localDishWeights, double[][] _regionMeans,
          double[] _kappa, double[] _nonToponymByDishDirichlet, double[][] _toponymCoordinateDirichlet) {
        globalDishWeightsFM = new double[_globalDishWeights.length];
        localDishWeightsFM = new double[_localDishWeights.length];
        kappaFM = new double[_kappa.length];
        nonToponymByDishDirichletFM = new double[_nonToponymByDishDirichlet.length];

        Arrays.fill(globalDishWeightsFM, 0);
        Arrays.fill(localDishWeightsFM, 0);
        Arrays.fill(kappaFM, 0);
        Arrays.fill(nonToponymByDishDirichletFM, 0);

        globalDishWeightsSM = new double[_globalDishWeights.length];
        localDishWeightsSM = new double[_localDishWeights.length];
        kappaSM = new double[_kappa.length];
        nonToponymByDishDirichletSM = new double[_nonToponymByDishDirichlet.length];

        Arrays.fill(globalDishWeightsSM, 0);
        Arrays.fill(localDishWeightsSM, 0);
        Arrays.fill(kappaSM, 0);
        Arrays.fill(nonToponymByDishDirichletSM, 0);

        regionMeansFM = new double[_regionMeans.length][];
//        regionMeansSM = new double[_regionMeans.length][];

        regionMeansFM = new double[_regionMeans.length][];
//        regionMeansSM = new double[_regionMeans.length][];
        geoMeanVecLen = _regionMeans[0].length;
        geoCovVecLen = geoMeanVecLen * (geoMeanVecLen + 1) / 2;
        for (int i = 0; i < _regionMeans.length; ++i) {
            double[] mean = new double[geoMeanVecLen];
            Arrays.fill(mean, 0);
            regionMeansFM[i] = mean;
            double[] secmo = new double[geoCovVecLen];
            Arrays.fill(secmo, 0);
//            regionMeansSM[i] = secmo;
        }

        toponymCoordinateDirichletFM = new double[_toponymCoordinateDirichlet.length][];
        toponymCoordinateDirichletSM = new double[_toponymCoordinateDirichlet.length][];
        for (int i = 0; i < _toponymCoordinateDirichlet.length; ++i) {
            int len = _toponymCoordinateDirichlet[i].length;
            toponymCoordinateDirichletFM[i] = new double[len];
            Arrays.fill(toponymCoordinateDirichletFM[i], 0);
            toponymCoordinateDirichletSM[i] = new double[len * (len + 1) / 2];
            Arrays.fill(toponymCoordinateDirichletSM[i], 0);
        }
    }

    protected void addToArrays(double[] _globalDishWeights, double[] _localDishWeights, double[][] _regionMeans,
          double[] _kappa, double[] _nonToponymByDishDirichlet, double[][] _toponymCoordinateDirichlet) {
        addToFirstMoment(globalDishWeightsFM, _globalDishWeights);
//        addToSecondMoment(globalDishWeightsSM, _globalDishWeights);
        addToFirstMoment(localDishWeightsFM, _localDishWeights);
//        addToSecondMoment(localDishWeightsSM, _localDishWeights);
        for (int l = 0; l < _regionMeans.length; ++l) {
            TGBLAS.daxpy(3, 1, _regionMeans[l], 1, regionMeansFM[l], 1);
//            addToFirstMoment(regionMeansFM[l], _regionMeans[l]);
//            addToCovariance(regionMeansFM[l], _regionMeans[l]);
        }
        addToFirstMoment(kappaFM, _kappa);
//        addToSecondMoment(kappaSM, _kappa);
        addToFirstMoment(nonToponymByDishDirichletFM, _nonToponymByDishDirichlet);
//        addToSecondMoment(nonToponymByDishDirichletSM, _nonToponymByDishDirichlet);
        for (int l = 0; l < _toponymCoordinateDirichlet.length; ++l) {
            addToFirstMoment(toponymCoordinateDirichletFM[l], _toponymCoordinateDirichlet[l]);
//            addToCovariance(toponymCoordinateDirichletSM[l], _toponymCoordinateDirichlet[l]);
        }
    }

    public void collectSamples(double[] _globalDishWeights, double[] _localDishWeights, double[][] _regionMeans,
          double[] _kappa, double[] _nonToponymByDishDirichlet, double[][] _toponymCoordinateDirichlet) {
        if (sampleCount < samples) {
            if (sampleiteration && (innerIter % lag == 0)) {
                sampleCount += 1;
                if (samples == sampleCount) {
                    finishedCollection = true;
                }

                System.err.print("(sample:" + sampleCount + ")");
                if (localDishWeightsFM == null) {
                    initializeCollectionArrays(_globalDishWeights, _localDishWeights, _regionMeans, _kappa, _nonToponymByDishDirichlet, _toponymCoordinateDirichlet);
                }
                addToArrays(_globalDishWeights, _localDishWeights, _regionMeans, _kappa, _nonToponymByDishDirichlet, _toponymCoordinateDirichlet);
            }
            if (finishedCollection) {
                averageSamples();
            }
        }
    }

    protected void averageSamples() {
        averageSamples(globalDishWeightsFM);
        averageSamples(localDishWeightsFM);
        for (int l = 0; l < regionMeansFM.length; ++l) {
            double nrm = TGBLAS.dnrm2(3, regionMeansFM[l], 1);
            try {
                for (int i = 0;; ++i) {
                    regionMeansFM[l][i] /= nrm;
                }
            } catch (ArrayIndexOutOfBoundsException e) {
            }
        }
        averageSamples(kappaFM);
        averageSamples(nonToponymByDishDirichletFM);

        for (int l = 0; l < toponymCoordinateDirichletFM.length; ++l) {
            averageSamples(toponymCoordinateDirichletFM[l]);
        }
    }

    public double[] getGlobalDishWeightsFM() {
        return globalDishWeightsFM;
    }

    public void setGlobalDishWeightsFM(double[] globalDishWeightsFM) {
        this.globalDishWeightsFM = globalDishWeightsFM;
    }

    public double[] getGlobalDishWeightsSM() {
        return globalDishWeightsSM;
    }

    public void setGlobalDishWeightsSM(double[] globalDishWeightsSM) {
        this.globalDishWeightsSM = globalDishWeightsSM;
    }

    public double[] getKappaFM() {
        return kappaFM;
    }

    public void setKappaFM(double[] kappaFM) {
        this.kappaFM = kappaFM;
    }

    public double[] getKappaSM() {
        return kappaSM;
    }

    public void setKappaSM(double[] kappaSM) {
        this.kappaSM = kappaSM;
    }

    public double[] getLocalDishWeightsFM() {
        return localDishWeightsFM;
    }

    public void setLocalDishWeightsFM(double[] localDishWeightsFM) {
        this.localDishWeightsFM = localDishWeightsFM;
    }

    public double[] getLocalDishWeightsSM() {
        return localDishWeightsSM;
    }

    public void setLocalDishWeightsSM(double[] localDishWeightsSM) {
        this.localDishWeightsSM = localDishWeightsSM;
    }

    public double[] getNonToponymByDishDirichletFM() {
        return nonToponymByDishDirichletFM;
    }

    public void setNonToponymByDishDirichletFM(double[] nonToponymByDishDirichletFM) {
        this.nonToponymByDishDirichletFM = nonToponymByDishDirichletFM;
    }

    public double[] getNonToponymByDishDirichletSM() {
        return nonToponymByDishDirichletSM;
    }

    public void setNonToponymByDishDirichletSM(double[] nonToponymByDishDirichletSM) {
        this.nonToponymByDishDirichletSM = nonToponymByDishDirichletSM;
    }

    public double[][] getRegionMeansFM() {
        return regionMeansFM;
    }

    public void setRegionMeansFM(double[][] regionMeansFM) {
        this.regionMeansFM = regionMeansFM;
    }

//    public double[][] getRegionMeansSM() {
//        return regionMeansSM;
//    }
//
//    public void setRegionMeansSM(double[][] regionMeansSM) {
//        this.regionMeansSM = regionMeansSM;
//    }
    public double[][] getToponymCoordinateWeightsFM() {
        return toponymCoordinateDirichletFM;
    }

    public void setToponymCoordinateWeightsFM(double[][] toponymCoordinateDirichletFM) {
        this.toponymCoordinateDirichletFM = toponymCoordinateDirichletFM;
    }

    public double[][] getToponymCoordinateWeightsSM() {
        return toponymCoordinateDirichletSM;
    }

    public void setToponymCoordinateWeightsSM(double[][] toponymCoordinateDirichletSM) {
        this.toponymCoordinateDirichletSM = toponymCoordinateDirichletSM;
    }
}
