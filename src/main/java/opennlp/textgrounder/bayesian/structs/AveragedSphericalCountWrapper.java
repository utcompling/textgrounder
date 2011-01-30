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
package opennlp.textgrounder.bayesian.structs;

import java.io.Serializable;
import opennlp.textgrounder.bayesian.spherical.models.SphericalModelBase;
import opennlp.textgrounder.bayesian.spherical.models.SphericalModelFields;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class AveragedSphericalCountWrapper extends SphericalModelFields implements Serializable {

    public AveragedSphericalCountWrapper(SphericalModelBase _regionModel) {
        averagedRegionCountsOfAllWords = _regionModel.getAveragedAllWordsRegionCounts();
        averagedRegionByDocumentCounts = _regionModel.getAveragedRegionByDocumentCounts();
        averagedTopicByDocumentCounts = _regionModel.getAveragedTopicByDocumentCounts();
        averagedWordByRegionCounts = _regionModel.getAveragedWordByRegionCounts();
        averagedRegionMeans = _regionModel.getAveragedRegionMeans();
        averagedToponymCoordinateWeights = _regionModel.getAveragedRegionToponymCoordinateCounts();

        toponymCoordinateLexicon = _regionModel.getToponymCoordinateLexicon();

        alpha_H = _regionModel.getAlpha();
//        beta = _regionModel.getBeta();
//        betaW = _regionModel.getBetaW();
//        kappa = _regionModel.getKappa();

        L = _regionModel.getL();
        D = _regionModel.getD();
        N = _regionModel.getN();
        T = _regionModel.getT();
        W = _regionModel.getW();
        
    }

    public void addHyperparameters() {
//        for (int i = 0; i < averagedRegionByDocumentCounts.length; ++i) {
//            averagedRegionByDocumentCounts[i] += alpha;
//        }
//        for (int i = 0; i < averagedWordByRegionCounts.length; ++i) {
//            averagedWordByRegionCounts[i] += beta;
//        }
//
//        for (int i = 0; i < averagedAllWordsRegionCounts.length; ++i) {
//            averagedAllWordsRegionCounts[i] += betaW;
//        }
    }
}
