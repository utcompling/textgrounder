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
package opennlp.textgrounder.bayesian.spherical.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;
import opennlp.textgrounder.bayesian.structs.AveragedSphericalCountWrapper;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class SphericalInternalToInternalInputReader extends SphericalInputReader {

    /**
     * 
     * @param _experimentParameters
     */
    public SphericalInternalToInternalInputReader(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        tokenArrayFile = new File(experimentParameters.getTokenArrayOutputPath());
        averagedCountsFile = new File(experimentParameters.getAveragedCountsPath());
    }

    public AveragedSphericalCountWrapper readProbabilities() {
        AveragedSphericalCountWrapper normalizedProbabilityWrapper = null;

        ObjectInputStream probIn = null;
        try {
            probIn = new ObjectInputStream(new GZIPInputStream(new FileInputStream(averagedCountsFile.getCanonicalPath())));
            normalizedProbabilityWrapper = (AveragedSphericalCountWrapper) probIn.readObject();
        } catch (IOException ex) {
            Logger.getLogger(SphericalInternalToInternalInputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(SphericalInternalToInternalInputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

        return normalizedProbabilityWrapper;
    }
}
