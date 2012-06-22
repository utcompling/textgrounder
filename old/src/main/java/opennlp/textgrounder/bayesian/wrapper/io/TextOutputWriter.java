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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import opennlp.textgrounder.bayesian.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.textstructs.TokenArrayBuffer;
import opennlp.textgrounder.bayesian.topostructs.ToponymToCoordinateMap;
import opennlp.textgrounder.bayesian.topostructs.ToponymToRegionIDsMap;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class TextOutputWriter extends OutputWriter {

    protected BufferedWriter tokenArrayWriter;
    protected BufferedWriter toponymRegionWriter;

    public TextOutputWriter(ConverterExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        openTokenArrayWriter();
        openToponymRegionWriter();
    }

    @Override
    public final void openTokenArrayWriter() {
        try {
            if (tokenArrayFile.getName().endsWith(".gz")) {
                tokenArrayWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tokenArrayFile))));
            } else {
                tokenArrayWriter = new BufferedWriter(new FileWriter(tokenArrayFile));
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TextOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(TextOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    @Override
    public final void openToponymRegionWriter() {
        try {
            if (toponymRegionFile.getName().endsWith(".gz")) {
                toponymRegionWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(toponymRegionFile))));
            } else {
                toponymRegionWriter = new BufferedWriter(new FileWriter(toponymRegionFile));
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TextOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(TextOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    @Override
    public void writeTokenArray(TokenArrayBuffer _tokenArrayBuffer) {
        try {
            for (int i = 0; i < _tokenArrayBuffer.size(); ++i) {
                int wordid = _tokenArrayBuffer.wordArrayList.get(i);
                int docid = _tokenArrayBuffer.documentArrayList.get(i);
                int stopstatus = _tokenArrayBuffer.stopwordArrayList.get(i);
                int topostatus = _tokenArrayBuffer.toponymArrayList.get(i);

                tokenArrayWriter.write(String.format("%d\t%d\t%d\t%d", wordid, docid, stopstatus, topostatus));
                tokenArrayWriter.newLine();
            }
        } catch (IOException ex) {
            Logger.getLogger(BinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

    }

    @Override
    public void writeToponymRegion(
          ToponymToRegionIDsMap _toponymToRegionIDsMap) {
        try {
            for (int topid : _toponymToRegionIDsMap.keySet()) {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append(topid).append("\t");
                HashSet<Integer> regids = _toponymToRegionIDsMap.get(topid);
                int fieldsize = regids.size();
                stringBuilder.append(fieldsize);
                for (int regid : regids) {
                    stringBuilder.append("\t").append(regid);
                }

                toponymRegionWriter.write(stringBuilder.toString());
                toponymRegionWriter.newLine();
            }

            toponymRegionWriter.close();
        } catch (IOException ex) {
            Logger.getLogger(BinaryOutputWriter.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    @Override
    public void openToponymCoordinateWriter() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void writeToponymCoordinate(ToponymToCoordinateMap _toponymToCoordinateMap) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
