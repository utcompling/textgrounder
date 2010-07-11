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
package opennlp.rlda.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import opennlp.rlda.apps.ExperimentParameters;
import opennlp.rlda.models.RegionModel;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class InputReader {

    /**
     *
     */
    protected RegionModel regionModel;
    /**
     *
     */
    protected ExperimentParameters experimentParameters;
    protected int D;
    protected int N;
    protected int W;
    protected int R;
    protected int[] wordVector;
    protected int[] regionVector;
    protected int[] documentVector;
    protected int[] toponymVector;
    protected int[] stopwordVector;
    protected int[] regionByToponymFilter;

    /**
     * 
     * @param _experimentParameters
     * @param _regionModel
     */
    public InputReader(ExperimentParameters _experimentParameters,
          RegionModel _regionModel) {
        experimentParameters = _experimentParameters;
        regionModel = _regionModel;
    }

    public void readTokenFile(File _file) throws FileNotFoundException,
          IOException {
        BufferedReader textIn = new BufferedReader(new FileReader(_file));

        HashSet<Integer> stopwordSet = new HashSet<Integer>();
        String line = null;
        ArrayList<Integer> wordArray = new ArrayList<Integer>(),
              docArray = new ArrayList<Integer>(),
              toponymArray = new ArrayList<Integer>(),
              stopwordArray = new ArrayList<Integer>();

        while ((line = textIn.readLine()) != null) {
            String[] fields = line.split("\\w+");
            if (fields.length > 2) {
                int wordidx = Integer.parseInt(fields[0]);
                wordArray.add(wordidx);
                int docidx = Integer.parseInt(fields[1]);
                docArray.add(docidx);
                toponymArray.add(Integer.parseInt(fields[2]));
                try {
                    stopwordArray.add(Integer.parseInt(fields[3]));
                    stopwordSet.add(wordidx);
                } catch (ArrayIndexOutOfBoundsException e) {
                }

                if (W < wordidx) {
                    W = wordidx;
                }
                if (D < docidx) {
                    D = docidx;
                }
            }
        }

        W -= stopwordSet.size();
        N = wordArray.size();

        wordVector = new int[N];
        copyToArray(wordVector, wordArray);

        documentVector = new int[N];
        copyToArray(documentVector, docArray);

        toponymVector = new int[N];
        copyToArray(toponymVector, toponymArray);

        stopwordVector = new int[N];
        if (stopwordArray.size() == N) {
            copyToArray(stopwordVector, stopwordArray);
        } else {
            for (int i = 0; i < N; ++i) {
                stopwordVector[i] = 0;
            }
        }

        regionVector = new int[N];
    }

    public void readRegionToponymFilter(File _file) throws FileNotFoundException,
          IOException {
        BufferedReader textin = new BufferedReader(new FileReader(_file));

        String line = null;

        regionByToponymFilter = new int[R * W];
        for (int i = 0; i < R * W; ++i) {
            regionByToponymFilter[i] = 0;
        }

        while ((line = textin.readLine()) != null) {
            if (!line.isEmpty()) {
                String[] fields = line.split("\\w+");

                int wordoff = Integer.parseInt(fields[0]) * R;
                for (int i = 1; i < fields.length; ++i) {
                    regionByToponymFilter[wordoff + i] = 1;
                }
            }
        }
    }

    public void getParameters() {
        regionModel.setD(D);
        regionModel.setN(N);
        regionModel.setR(R);
        regionModel.setW(W);
        regionModel.setDocumentVector(documentVector);
    }

    /**
     * Copy a sequence of numbers from ta to array ia.
     *
     * @param <T>   Any number type
     * @param ia    Target array of integers to be copied to
     * @param ta    Source List<T> of numbers to be copied from
     */
    protected static <T extends Number> void copyToArray(int[] ia, ArrayList<T> ta) {
        for (int i = 0; i < ta.size(); ++i) {
            ia[i] = ta.get(i).intValue();
        }
    }
}
