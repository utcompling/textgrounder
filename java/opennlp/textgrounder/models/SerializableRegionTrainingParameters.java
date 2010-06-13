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
package opennlp.textgrounder.models;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import opennlp.textgrounder.textstructs.Lexicon;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class SerializableRegionTrainingParameters implements Serializable {

    protected SerializableRegionTrainingParameters loadBuffer = null;
    /**
     * Vector of document indices
     */
    protected int[] documentVector;
    /**
     * Vector of word indices
     */
    protected int[] wordVector;
    /**
     * Vector of topics
     */
    protected int[] topicVector;
    /**
     * Counts of topics
     */
    protected int[] topicCounts;
    /**
     * 
     */
    protected Lexicon lexicon;

    /**
     * Loads parameters stored in a file given a path and a RegionModel instance.
     *
     * @param filename Path to stored model
     * @param rm RegionModel instance that holds the loaded parameters as fields.
     * @throws IOException
     */
    public void loadParameters(String filename, RegionModelSerializer rm) throws
          IOException {
        ObjectInputStream modelIn =
              new ObjectInputStream(new GZIPInputStream(new FileInputStream(filename)));
        try {
            loadBuffer = (SerializableRegionTrainingParameters) modelIn.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        rm.lexicon = loadBuffer.lexicon;
    }

    /**
     * Stores parameters learned by running RegionModel on training data. The
     * parameters are saved to the designated path.
     *
     * @param filename Path to store parameters to
     * @param rm RegionModel instance that holds the learned parameters as fields.
     * @throws IOException
     */
    public void saveParameters(String filename, RegionModelSerializer rm) throws
          IOException {
        lexicon = rm.lexicon;

        ObjectOutputStream modelOut =
              new ObjectOutputStream(new GZIPOutputStream(new FileOutputStream(filename + ".gz")));
        modelOut.writeObject(this);
        modelOut.close();
    }
}
