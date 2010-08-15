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

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class ConverterExperimentParameters extends ExperimentParameters {

    /**
     * Path to file that has been converted to internal format
     *
     * @return
     */
    public String getInputPath() {
        return joinPath(projectRoot, corpusFilename);
    }

    public String getOutputPath() {
        return joinPath(projectRoot, processedCorpusFilename);
    }

    public String getLexiconPath() {
        return joinPath(projectRoot,lexiconFilename);
    }

    public String getRegionPath() {
        return joinPath(projectRoot, regionFilename);
    }

    public int getCountCutoff() {
        return countCutoff;
    }

    public int getDegreesPerRegion() {
        return degreesPerRegion;
    }
}
