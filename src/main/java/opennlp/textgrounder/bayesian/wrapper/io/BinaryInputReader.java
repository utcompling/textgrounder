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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class BinaryInputReader extends InputReader {

    protected DataInputStream tokenArrayInputStream;

    public BinaryInputReader(ConverterExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        openTokenArrayReader();
    }

    @Override
    public final void openTokenArrayReader() {
        try {
            if (tokenArrayFile.getName().endsWith(".gz")) {
                tokenArrayInputStream = new DataInputStream(
                      new BufferedInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(tokenArrayFile)))));
            } else {
                tokenArrayInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(tokenArrayFile)));
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(BinaryInputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(BinaryInputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    @Override
    public int[] nextTokenArrayRecord() throws EOFException, IOException {
        int[] record = new int[5];

        int wordid = tokenArrayInputStream.readInt();
        int docid = tokenArrayInputStream.readInt();
        int topstatus = (int) tokenArrayInputStream.readByte();
        int stopstatus = (int) tokenArrayInputStream.readByte();
        int regid = /*(int)*/ tokenArrayInputStream.readInt();
        record[0] = wordid;
        record[1] = docid;
        record[2] = topstatus;
        record[3] = stopstatus;
        record[4] = regid;
        return record;
    }
}
