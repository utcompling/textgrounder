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
package opennlp.textgrounder.bayesian.rlda.structs;

import java.io.Serializable;
import opennlp.textgrounder.bayesian.rlda.models.RegionModel;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class NormalizedProbabilityWrapper implements Serializable {

    /**
     * Hyperparameter for region*doc priors
     */
    public double alpha;
    /**
     * Hyperparameter for word*region priors
     */
    public double beta;
    /**
     * Normalization term for word*region gibbs sampler
     */
    public double betaW;
    /**
     * Number of documents
     */
    public int D;
    /**
     * Number of tokens
     */
    public int N;
    /**
     * Number of regions
     */
    public int R;
    /**
     * Number of non-stopword word types. Equivalent to <p>fW-sW</p>.
     */
    public int W;
    /**
     *
     */
    public double[] normalizedRegionCounts;
    /**
     *
     */
    public double[] normalizedWordByRegionCounts;
    /**
     *
     */
    public double[] normalizedRegionByDocumentCounts;

    public NormalizedProbabilityWrapper() {
    }

    public NormalizedProbabilityWrapper(RegionModel _regionModel) {
        normalizedRegionCounts = _regionModel.getNormalizedRegionCounts();
        normalizedWordByRegionCounts = _regionModel.getNormalizedWordByRegionCounts();
        normalizedRegionByDocumentCounts = _regionModel.getNormalizedRegionByDocumentCounts();

        alpha = _regionModel.getAlpha();
        beta = _regionModel.getBeta();
        betaW = _regionModel.getBetaW();
        D = _regionModel.getD();
        N = _regionModel.getN();
        R = _regionModel.getR();
        W = _regionModel.getW();
    }

    public void addHyperparameters() {

        for (int i = 0; i < normalizedRegionByDocumentCounts.length; ++i) {
            normalizedRegionByDocumentCounts[i] += alpha;
        }
        for (int i = 0; i < normalizedWordByRegionCounts.length; ++i) {
            normalizedWordByRegionCounts[i] += beta;
        }
        for (int i = 0; i < normalizedRegionCounts.length; ++i) {
            normalizedRegionCounts[i] += betaW;
        }
    }
}
