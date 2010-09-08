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
package opennlp.textgrounder.bayesian.rlda.io;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class RLDAInputReader extends RLDAIOBase {

    /**
     * 
     * @param _experimentParameters
     */
    public RLDAInputReader(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        tokenArrayFile = new File(experimentParameters.getTokenArrayInputPath());
    }

    /**
     *
     * @return
     * @throws EOFException
     */
    public abstract int[] nextTokenArrayRecord() throws EOFException,
          IOException;

    /**
     * 
     * @return
     * @throws EOFException
     */
    public abstract int[] nextToponymRegionFilter() throws EOFException,
          IOException;

    /**
     *
     */
    public abstract void openTokenArrayReader();

    /**
     * 
     */
    public abstract void openToponymRegionReader();

    /**
     *
     */
    public abstract void closeTokenArrayReader();

    /**
     *
     */
    public abstract void closeToponymRegionReader();

    /**
     *
     */
    public abstract void resetTokenArrayReader();

    /**
     *
     */
    public abstract void resetToponymRegionReader();

    /**
     * 
     * @return
     */
    public abstract int getMaxRegionID();
}
