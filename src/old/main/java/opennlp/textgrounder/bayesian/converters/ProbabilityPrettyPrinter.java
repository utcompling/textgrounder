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
package opennlp.textgrounder.bayesian.converters;

import opennlp.textgrounder.bayesian.apps.*;
import opennlp.textgrounder.bayesian.textstructs.Lexicon;
import opennlp.textgrounder.bayesian.wrapper.io.*;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public abstract class ProbabilityPrettyPrinter {

    protected final static double EPSILON = 1e-10;
    /**
     * Hyperparameter for region*doc priors
     */
    protected double alpha;
    /**
     * Hyperparameter for word*region priors
     */
    protected double beta;
    /**
     * Normalization term for word*region gibbs sampler
     */
    protected double betaW;
    /**
     * Number of documents
     */
    protected int D;
    /**
     * Number of tokens
     */
    protected int N;
    /**
     * Number of regions
     */
    protected int L;
    /**
     * Number of non-stopword word types. Equivalent to <p>fW-sW</p>.
     */
    protected int W;
    /**
     * 
     */
    protected Lexicon lexicon = null;
    /**
     *
     */
    protected ConverterExperimentParameters experimentParameters = null;
    /**
     *
     */
    protected InputReader inputReader = null;

    public ProbabilityPrettyPrinter(ConverterExperimentParameters _parameters) {
        experimentParameters = _parameters;
        inputReader = new BinaryInputReader(experimentParameters);
    }

    public abstract void readFiles();

    public abstract void normalizeAndPrintXMLProbabilities();

    public abstract void normalizeAndPrintWordByRegion();

    public abstract void normalizeAndPrintRegionByWord();

    public abstract void normalizeAndPrintRegionByDocument();

    protected boolean isInvalidProb(double _val) {
        if (_val > EPSILON && !Double.isNaN(_val)) {
            return false;
        }
        return true;
    }

    public void normalizeAndPrintAll() {
        normalizeAndPrintRegionByDocument();
        normalizeAndPrintRegionByWord();
        normalizeAndPrintWordByRegion();
    }
}
