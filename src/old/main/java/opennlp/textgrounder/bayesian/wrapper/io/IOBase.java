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
package opennlp.textgrounder.bayesian.wrapper.io;

import java.io.File;
import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class IOBase {

    /**
     * File of tokens, toponym status, stopword status and whatnot
     */
    protected File tokenArrayFile;
    /**
     * File linking toponym indexes to regions
     */
    protected File toponymRegionFile;
    /**
     * File linking toponym coordinates to regions
     */
    protected File toponymCoordinateFile;
    /**
     * File for lexicon dump
     */
    protected File lexiconFile;
    /**
     * File for 2D region array dump
     */
    protected File regionFile;
    /**
     * 
     */
    protected File probabilityFile;
    /**
     *
     */
    protected ConverterExperimentParameters experimentParameters;

    public IOBase(ConverterExperimentParameters _experimentParameters) {
        experimentParameters = _experimentParameters;
        toponymRegionFile = new File(experimentParameters.getToponymRegionPath());
        toponymCoordinateFile = new File(experimentParameters.getToponymCoordinatePath());
        lexiconFile = new File(experimentParameters.getLexiconPath());
        regionFile = new File(experimentParameters.getRegionPath());
    }
}
