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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalBinaryOutputWriter extends SphericalOutputWriter {

    /**
     * 
     */
    protected DataOutputStream tokenArrayOutputStream;

    /**
     * 
     * @param _experimentParameters
     */
    public SphericalBinaryOutputWriter(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        openTokenArrayWriter();
    }

    /**
     *
     */
    @Override
    public final void openTokenArrayWriter() {
        try {
            if (tokenArrayFile.getName().endsWith(".gz")) {
                tokenArrayOutputStream = new DataOutputStream(
                      new BufferedOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(tokenArrayFile)))));
            } else {
                tokenArrayOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tokenArrayFile)));
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(SphericalBinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(SphericalBinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    /**
     *
     */
    @Override
    public void writeTokenArray(int[] _wordVector, int[] _documentVector,
          int[] _toponymVector, int[] _stopwordVector, int[] _regionVector, int[] _coordVector) {
        try {
            for (int i = 0; i < _wordVector.length; ++i) {
                int wordid = _wordVector[i];
                tokenArrayOutputStream.writeInt(wordid);
                int docid = _documentVector[i];
                tokenArrayOutputStream.writeInt(docid);
                byte topostatus = (byte) _toponymVector[i];
                tokenArrayOutputStream.writeByte(topostatus);
                byte stopstatus = (byte) _stopwordVector[i];
                tokenArrayOutputStream.writeByte(stopstatus);
                int regid = _regionVector[i];
                tokenArrayOutputStream.writeInt(regid);
                int coordid = _coordVector[i];
                tokenArrayOutputStream.writeInt(coordid);
            }

            tokenArrayOutputStream.close();
        } catch (IOException ex) {
            Logger.getLogger(SphericalBinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }
}
