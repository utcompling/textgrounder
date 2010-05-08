///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.models;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import opennlp.textgrounder.textstructs.Lexicon;
import opennlp.textgrounder.topostructs.Region;

/**
 *
 * @author tsmoon
 */
public class SerializableParameters implements Serializable {

    static private final long serialVersionUID = 13114338L;
    /**
     *
     */
    protected SerializableParameters loadBuffer = null;
    /**
     * Training data
     */
    protected String inputPath;
    /**
     * Table from index to region
     */
    Map<Integer, Region> regionMap;
    /**
     * Probability of topics
     */
    protected double[] topicProbs;
    /**
     * Probability of word given topic. since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected double[] wordByTopicProbs;
    /**
     * Collection of training data
     */
    protected Lexicon lexicon;

    public void loadParameters(String filename, RegionModel rm) throws
          IOException {
        ObjectInputStream modelIn =
              new ObjectInputStream(new GZIPInputStream(new FileInputStream(filename)));
        try {
            loadBuffer = (SerializableParameters) modelIn.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        rm.inputPath = loadBuffer.inputPath;
        rm.regionMap = loadBuffer.regionMap;
        rm.topicProbs = loadBuffer.topicProbs;
        rm.wordByTopicProbs = loadBuffer.wordByTopicProbs;
        rm.lexicon = loadBuffer.lexicon;
        loadBuffer = null;
    }

    public void saveParameters(String filename, RegionModel rm) throws
          IOException {
        inputPath = rm.inputPath;
        regionMap = rm.regionMap;
        topicProbs = rm.topicProbs;
        wordByTopicProbs = rm.wordByTopicProbs;
        lexicon = rm.lexicon;

        ObjectOutputStream modelOut =
              new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(filename + ".gz")));
        modelOut.writeObject(this);
        modelOut.close();
    }
}
