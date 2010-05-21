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

import gnu.trove.TIntObjectHashMap;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import opennlp.textgrounder.textstructs.Lexicon;
import opennlp.textgrounder.topostructs.Region;

/**
 * Serializable class for storing lexicon, path to input, regions and coordinates,
 * probabilities of topics and probabilities of words given topics. The parameters
 * are trained from the RegionModel. The parameters can then be read in from
 * the command line through TopicParameterToKML and used to generate kml files
 * that visualize the probabilities of individual words given regions.
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
     * Location of training data. Not actually used to read data. Merely
     * required to generate kml files that include trainInputPath in the header
     */
    protected String inputPath;
    /**
     * Table from index to region
     */
    TIntObjectHashMap<Region> regionMap;
    /**
     * Probability of topics (regions)
     */
    protected double[] topicProbs;
    /**
     * Probability of word given topic. since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected double[] wordByTopicProbs;
    /**
     * Lexicon that stores mappings from word indices to words and back.
     */
    protected Lexicon lexicon;

    /**
     * Loads parameters stored in a file given a path and a RegionModel instance.
     * 
     * @param filename Path to stored model
     * @param rm RegionModel instance that holds the loaded parameters as fields.
     * @throws IOException
     */
    public void loadParameters(String filename, RegionModel rm) throws
          IOException {
        ObjectInputStream modelIn =
              new ObjectInputStream(new GZIPInputStream(new FileInputStream(filename)));
        try {
            loadBuffer = (SerializableParameters) modelIn.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        rm.trainInputPath = loadBuffer.inputPath;
        rm.regionMap = loadBuffer.regionMap;
        rm.topicProbs = loadBuffer.topicProbs;
        rm.wordByTopicProbs = loadBuffer.wordByTopicProbs;
        rm.lexicon = loadBuffer.lexicon;
        loadBuffer = null;
    }

    /**
     * Stores parameters learned by running RegionModel on training data. The
     * parameters are saved to the designated path.
     *
     * @param filename Path to store parameters to
     * @param rm RegionModel instance that holds the learned parameters as fields.
     * @throws IOException
     */
    public void saveParameters(String filename, RegionModel rm) throws
          IOException {
        inputPath = rm.trainInputPath;
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
