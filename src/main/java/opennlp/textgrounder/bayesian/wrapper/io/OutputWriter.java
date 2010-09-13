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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.textstructs.Lexicon;
import opennlp.textgrounder.bayesian.textstructs.TokenArrayBuffer;
import opennlp.textgrounder.bayesian.topostructs.Region;
import opennlp.textgrounder.bayesian.topostructs.ToponymToCoordinateMap;
import opennlp.textgrounder.bayesian.topostructs.ToponymToRegionIDsMap;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class OutputWriter extends IOBase {

    /**
     *
     * @param _experimentParameters
     */
    public OutputWriter(ConverterExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        tokenArrayFile = new File(experimentParameters.getTokenArrayInputPath());
    }

    /**
     *
     */
    public abstract void openTokenArrayWriter();

    /**
     *
     */
    public abstract void openToponymRegionWriter();

    /**
     *
     */
    public abstract void openToponymCoordinateWriter();

    /**
     *
     */
    public abstract void writeTokenArray(
          TokenArrayBuffer _tokenArrayBuffer);

    /**
     *
     */
    public abstract void writeToponymRegion(
          ToponymToRegionIDsMap _toponymToRegionIDsMap);

    /**
     *
     */
    public abstract void writeToponymCoordinate(
          ToponymToCoordinateMap _toponymToCoordinateMap);

    public void writeLexicon(Lexicon _lexicon) {

        ObjectOutputStream lexiconOut = null;
        try {
            lexiconOut = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(lexiconFile.getCanonicalPath())));
            lexiconOut.writeObject(_lexicon);
            lexiconOut.close();
        } catch (IOException ex) {
            Logger.getLogger(OutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    public void writeRegions(Region[][] regions) {
        ObjectOutputStream regionOut = null;
        try {
            regionOut = new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(regionFile.getCanonicalPath())));
            regionOut.writeObject(regions);
            regionOut.close();
        } catch (IOException ex) {
            Logger.getLogger(OutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }
}
