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
package opennlp.textgrounder.bayesian.rlda.wrapper.io;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import opennlp.textgrounder.bayesian.rlda.apps.ConverterExperimentParameters;
import opennlp.textgrounder.bayesian.rlda.structs.NormalizedProbabilityWrapper;
import opennlp.textgrounder.bayesian.rlda.textstructs.Lexicon;
import opennlp.textgrounder.bayesian.rlda.topostructs.Region;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class InputReader extends IOBase {

    public InputReader(ConverterExperimentParameters _experimentParameters) {
        super(_experimentParameters);
        tokenArrayFile = new File(experimentParameters.getTokenArrayOutputPath());
        probabilityFile = new File(experimentParameters.getSampledProbabilitiesPath());
    }

    public Lexicon readLexicon() {
        ObjectInputStream lexiconIn = null;
        Lexicon lexicon = null;
        try {
            lexiconIn = new ObjectInputStream(new GZIPInputStream(new FileInputStream(lexiconFile.getCanonicalPath())));
            lexicon = (Lexicon) lexiconIn.readObject();
        } catch (IOException ex) {
            Logger.getLogger(InputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(InputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
        return lexicon;
    }

    public Region[][] readRegions() {
        ObjectInputStream regionIn = null;
        Region[][] regionMatrix = null;
        try {
            regionIn = new ObjectInputStream(new GZIPInputStream(new FileInputStream(regionFile.getCanonicalPath())));
            regionMatrix = (Region[][]) regionIn.readObject();
        } catch (IOException ex) {
            Logger.getLogger(InputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(InputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
        return regionMatrix;
    }

    public NormalizedProbabilityWrapper readProbabilities() {
        NormalizedProbabilityWrapper normalizedProbabilityWrapper = null;

        ObjectInputStream probIn = null;
        try {
            probIn = new ObjectInputStream(new GZIPInputStream(new FileInputStream(probabilityFile.getCanonicalPath())));
            normalizedProbabilityWrapper = (NormalizedProbabilityWrapper) probIn.readObject();
        } catch (IOException ex) {
            Logger.getLogger(InputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(InputReader.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }

        return normalizedProbabilityWrapper;
    }

    public abstract void openTokenArrayReader();

    public abstract int[] nextTokenArrayRecord() throws EOFException,
          IOException;
}
