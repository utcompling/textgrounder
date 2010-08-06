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
package opennlp.rlda.converters;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import opennlp.rlda.apps.ConverterExperimentParameters;
import opennlp.rlda.apps.ExperimentParameters;
import opennlp.rlda.structs.IntDoublePair;
import opennlp.rlda.wrapper.io.InputReader;
import opennlp.rlda.structs.NormalizedProbabilityWrapper;
import opennlp.rlda.textstructs.Lexicon;
import opennlp.rlda.topostructs.Region;
import opennlp.rlda.wrapper.io.BinaryInputReader;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class ProbabilityPrettyPrinter {

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
    protected int R;
    /**
     * Number of non-stopword word types. Equivalent to <p>fW-sW</p>.
     */
    protected int W;
    /**
     *
     */
    protected double[] normalizedRegionCounts;
    /**
     *
     */
    protected double[] normalizedWordByRegionCounts;
    /**
     *
     */
    protected double[] normalizedRegionByDocumentCounts;
    /**
     * 
     */
    protected Lexicon lexicon = null;
    protected Region[][] regionMatrix = null;
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

    public void readFiles() {
        NormalizedProbabilityWrapper normalizedProbabilityWrapper = inputReader.readProbabilities();

        alpha = normalizedProbabilityWrapper.alpha;
        beta = normalizedProbabilityWrapper.beta;
        betaW = normalizedProbabilityWrapper.betaW;
        D = normalizedProbabilityWrapper.D;
        N = normalizedProbabilityWrapper.N;
        R = normalizedProbabilityWrapper.R;
        W = normalizedProbabilityWrapper.W;
        normalizedRegionByDocumentCounts = normalizedProbabilityWrapper.normalizedRegionByDocumentCounts;
        normalizedRegionCounts = normalizedProbabilityWrapper.normalizedRegionCounts;
        normalizedWordByRegionCounts = normalizedProbabilityWrapper.normalizedWordByRegionCounts;

        lexicon = inputReader.readLexicon();
        regionMatrix = inputReader.readRegions();
    }

    /**
     * Create two normalized probability tables, {@link #TopWordsPerTopic} and
     * {@link #topicProbs}. {@link #topicProbs} overwrites previous values.
     * {@link #TopWordsPerTopic} only retains first {@link #outputPerTopic}
     * words and values.
     */
    public void normalizeAndPrintWordByRegion() {

//        wordByRegionProbs = new double[W * R];

        topWordsPerRegion = new IntDoublePair[R][];
        for (int i = 0; i < R; ++i) {
            topWordsPerRegion[i] = new IntDoublePair[outputPerClass];
        }

        regionProbs = new double[R];

        Double sum = 0.;
        for (int i = 0; i < R; ++i) {
            sum += regionProbs[i] = normalizedRegionCounts[i] + betaW;
            ArrayList<IntDoublePair> topWords = new ArrayList<IntDoublePair>();
            for (int j = 0; j < W; ++j) {
                topWords.add(new IntDoublePair(j, normalizedWordByRegionCounts[j * R + i] + beta));
                wordByRegionProbs[j * R + i] = (normalizedWordByRegionCounts[j * R + i] + beta) / regionProbs[i];
            }
            Collections.sort(topWords);
            int j = 0;
            try {
                for (; j < outputPerClass; ++j) {
                    topWordsPerRegion[i][j] =
                          new IntDoublePair(topWords.get(j).wordid, topWords.get(j).count / regionProbs[i]);
                }
            } catch (IndexOutOfBoundsException e) {
                for (; j < outputPerClass; ++j) {
                    topWordsPerRegion[i][j] = new IntDoublePair(-1, 0);
                }
            }
        }

        for (int i = 0; i < R; ++i) {
            regionProbs[i] /= sum;
        }
    }
//    /**
//     * Print the normalized sample counts for each topic to out. Print only the top {@link
//     * #outputPerTopic} per given topic.
//     *
//     * @param out
//     * @throws IOException
//     */
//    protected void printTopics(BufferedWriter out) throws IOException {
//        int startt = 0, M = 4, endt = Math.min(M + startt, topicProbs.length);
//        out.write("***** Word Probabilities by Topic *****\n\n");
//        while (startt < T) {
//            for (int i = startt; i < endt; ++i) {
//                String header = "T_" + i;
//                header = String.format("%25s\t%6.5f\t", header, topicProbs[i]);
//                out.write(header);
//            }
//
//            out.newLine();
//            out.newLine();
//
//            for (int i = 0;
//                  i < outputPerClass; ++i) {
//                for (int c = startt; c < endt; ++c) {
//                    String line = String.format("%25s\t%6.5f\t",
//                          topWordsPerTopic[c][i].stringValue,
//                          topWordsPerTopic[c][i].doubleValue);
//                    out.write(line);
//                }
//                out.newLine();
//            }
//            out.newLine();
//            out.newLine();
//
//            startt = endt;
//            endt = java.lang.Math.min(T, startt + M);
//        }
//    }
}
