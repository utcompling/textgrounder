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
package opennlp.textgrounder.bayesian.apps;

import opennlp.textgrounder.bayesian.spherical.models.*;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class TrainSphericalModel extends BaseApp {

    public static void main(String[] args) {

        ExperimentParameters experimentParameters = new ExperimentParameters();
        processRawCommandline(args, experimentParameters);

        SphericalModelBase smb = null;

        switch (experimentParameters.getModelType()) {
            case V1:
            case V1_INDEPENDENT_REGIONS:
                smb = new SphericalModelV1(experimentParameters);
                break;
            case V2:
            case V2_DEPENDENT_REGIONS:
                smb = new SphericalModelV2(experimentParameters);
                break;
            case V3:
            case V3_DEPENDENT_REGIONS:
                smb = new SphericalModelV3(experimentParameters);
                break;
            default:
                System.err.println(experimentParameters.getModelType().toString() + " is an invalid model for this experiment. Choose V1, V2 or V3.");
                System.exit(1);
                break;
        }

        smb.initialize();
        smb.train();
        smb.decode();
        smb.write();
    }
}
