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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import opennlp.textgrounder.bayesian.apps.ExperimentParameters;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SphericalGlobalToInternalBinaryInputReader extends SphericalGlobalToInternalInputReader {

    protected DataInputStream tokenArrayInputStream;
    protected DataInputStream toponymCoordinateInputStream;

    public SphericalGlobalToInternalBinaryInputReader(ExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        openTokenArrayReader();
        openToponymCoordinateReader();
    }

    @Override
    public int[] nextTokenArrayRecord() throws EOFException, IOException {
        int[] record = new int[6];

        int wordid = tokenArrayInputStream.readInt();
        int docid = tokenArrayInputStream.readInt();
        int topstatus = (int) tokenArrayInputStream.readByte();
        int stopstatus = (int) tokenArrayInputStream.readByte();
        record[0] = wordid;
        record[1] = docid;
        record[2] = topstatus;
        record[3] = stopstatus;
        return record;
    }

    /**
     *
     * @return
     * @throws EOFException
     * @throws IOException
     */
    @Override
    public ArrayList<Object> nextToponymCoordinateRecord() throws EOFException,
          IOException {

        ArrayList<Object> toprecord = new ArrayList<Object>();

        int topid = toponymCoordinateInputStream.readInt();
        toprecord.add(new Integer(topid));

        int fieldsize = toponymCoordinateInputStream.readInt();
        double[] record = new double[fieldsize];
        for (int i = 0; i < fieldsize; i++) {
            record[i] = /*(double)*/ toponymCoordinateInputStream.readDouble();
//            record[i + 1] = (double) toponymCoordinateInputStream.readDouble();
        }
        toprecord.add(record);

        return toprecord;
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
            Logger.getLogger(SphericalGlobalToInternalBinaryInputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(SphericalGlobalToInternalBinaryInputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    @Override
    public final void openToponymCoordinateReader() {
        try {
            if (toponymCoordinateFile.getName().endsWith(".gz")) {
                toponymCoordinateInputStream = new DataInputStream(
                      new BufferedInputStream(new GZIPInputStream(new BufferedInputStream(new FileInputStream(toponymCoordinateFile)))));
            } else {
                toponymCoordinateInputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(toponymCoordinateFile)));
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(SphericalGlobalToInternalBinaryInputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(SphericalGlobalToInternalBinaryInputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    @Override
    public void closeTokenArrayReader() {
        try {
            tokenArrayInputStream.close();
        } catch (IOException ex) {
        }
    }

    @Override
    public void closeToponymCoordinateReader() {
        try {
            toponymCoordinateInputStream.close();
        } catch (IOException ex) {
        }
    }

    @Override
    public void resetTokenArrayReader() {
        closeTokenArrayReader();
        openTokenArrayReader();
    }

    @Override
    public void resetToponymCoordinateReader() {
        closeToponymCoordinateReader();
        openToponymCoordinateReader();
    }
}
