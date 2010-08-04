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
package opennlp.rlda.apps;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class ConverterExperimentParameters extends ExperimentParameters {

    public int getCountCutoff() {
        return countCutoff;
    }

    public int getDegreesPerRegion() {
        return degreesPerRegion;
    }

    public String getInputPath() {
        try {
            return joinPath(corpusPath, corpusFilename);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ConverterExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ConverterExperimentParameters.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }
}
