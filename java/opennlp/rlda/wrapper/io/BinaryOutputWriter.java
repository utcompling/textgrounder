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
package opennlp.rlda.wrapper.io;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import opennlp.rlda.apps.ConverterExperimentParameters;
import opennlp.rlda.textstructs.TokenArrayBuffer;
import opennlp.rlda.topostructs.ToponymToRegionIDsMap;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class BinaryOutputWriter extends OutputWriter {

    protected DataOutputStream tokenArrayOutputStream;
    protected DataOutputStream toponymRegionOutputStream;

    public BinaryOutputWriter(ConverterExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        openTokenArrayWriter();
        openToponymRegionWriter();
    }

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
            Logger.getLogger(BinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(BinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    @Override
    public final void openToponymRegionWriter() {
        try {
            if (toponymRegionFile.getName().endsWith(".gz")) {
                toponymRegionOutputStream = new DataOutputStream(
                      new BufferedOutputStream(new GZIPOutputStream(new BufferedOutputStream(new FileOutputStream(toponymRegionFile)))));
            } else {
                toponymRegionOutputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(toponymRegionFile)));
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(BinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(BinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

    }

    @Override
    public void writeTokenArrayWriter(TokenArrayBuffer _tokenArrayBuffer) {
        try {
            for (int i = 0; i < _tokenArrayBuffer.size(); ++i) {
                int wordid = _tokenArrayBuffer.wordArrayList.get(i);
                tokenArrayOutputStream.writeInt(wordid);
                int docid = _tokenArrayBuffer.documentArrayList.get(i);
                tokenArrayOutputStream.writeInt(docid);
                byte stopstatus = (byte) (int) _tokenArrayBuffer.stopwordArrayList.get(i);
                tokenArrayOutputStream.writeByte(stopstatus);
                byte topostatus = (byte) (int) _tokenArrayBuffer.toponymArrayList.get(i);
                tokenArrayOutputStream.writeByte(topostatus);
            }

            tokenArrayOutputStream.close();
        } catch (IOException ex) {
            Logger.getLogger(BinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    @Override
    public void writeToponymRegionWriter(
          ToponymToRegionIDsMap _toponymToRegionIDsMap) {
        
        try {
            for (int topid : _toponymToRegionIDsMap.keySet()) {
                HashSet<Integer> regids = _toponymToRegionIDsMap.get(topid);
                int fieldsize = regids.size();
                toponymRegionOutputStream.writeInt(topid);
                toponymRegionOutputStream.writeInt(fieldsize);
                for (int regid : regids) {
                    toponymRegionOutputStream.writeInt(regid);
                }
            }

            toponymRegionOutputStream.close();
        } catch (IOException ex) {
            Logger.getLogger(BinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }
}
