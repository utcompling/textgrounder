///////////////////////////////////////////////////////////////////////////////
//  Copyright (C) 2010 Taesun Moon, The University of Texas at Austin
//
//  This library is free software; you can redistribute it and/or
//  modify it under the terms of the GNU Lesser General Public
//  License as published by the Free Software Foundation; either
//  version 3 of the License, or (at your option) any later version.
//
//  This library is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
//
//  You should have received a copy of the GNU Lesser General Public
//  License along with this program; if not, write to the Free Software
//  Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
///////////////////////////////////////////////////////////////////////////////
package opennlp.textgrounder.models;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import opennlp.textgrounder.annealers.*;
import opennlp.textgrounder.ec.util.MersenneTwisterFast;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.io.DocumentSet;
import opennlp.textgrounder.util.*;

/**
 * Basic topic model implementation.
 *
 * @author tsmoon
 */
public class TopicModel extends Model {

    /**
     * Random number generator. Implements the fast Mersenne Twister.
     */
    protected MersenneTwisterFast rand;
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
     * Counts of tcount per topic. However, since access more often occurs in
     * terms of the tcount, it will be a topic by word matrix.
     */
    protected int[] wordByTopicCounts;
    /**
     * Counts of topics per document
     */
    protected int[] topicByDocumentCounts;
    /**
     * Hyperparameter for topic*doc priors
     */
    protected double alpha;
    /**
     * Hyperparameter for word*topic priors
     */
    protected double beta;
    /**
     * Normalization term for word*topic gibbs sampler
     */
    protected double betaW;
    /**
     * Number of topics
     */
    protected int T;
    /**
     * Number of word types
     */
    protected int W;
    /**
     * Number of documents
     */
    protected int D;
    /**
     * Number of tokens
     */
    protected int N;
    /**
     * Handles simulated annealing, burn-in, and full sampling cycle
     */
    protected Annealer annealer;
    /**
     * Number of types (either word or morpheme) to print per state or topic
     */
    protected int outputPerClass;
    /**
     * Posterior probabilities for topics.
     */
    protected double[] topicProbs;
    /**
     * Table of top {@link #outputPerTopic} words per topic. Used in
     * normalization and printing.
     */
    protected StringDoublePair[][] topWordsPerTopic;
    /**
     * Output buffer to write normalized, tabulated data to.
     */
    protected BufferedWriter tabulatedOutput;

    /**
     * This is not the default constructor. It should only be called by
     * constructors of derived classes if necessary.
     */
    protected TopicModel() {
    }

    /**
     * Default constructor. Allocates memory for all the fields.
     *
     * @param docSet Container holding training data. In the form of arrays of
     *              arrays of word indices
     * @param T Number of topics
     */
    public TopicModel(DocumentSet docSet, CommandLineOptions options) {
        this.docSet = docSet;
        initializeFromOptions(options);
        allocateFields(docSet, T);
    }

    /**
     * Allocate memory for fields
     *
     * @param docSet Container holding training data.
     * @param T Number of topics
     */
    protected void allocateFields(DocumentSet docSet, int T) {
        N = 0;
        W = docSet.wordsToInts.size();
        D = docSet.size();
        betaW = beta * W;
        for (int i = 0; i < docSet.size(); i++) {
            N += docSet.get(i).size();
        }

        documentVector = new int[N];
        wordVector = new int[N];
        topicVector = new int[N];
        for (int i = 0; i < N; ++i) {
            documentVector[i] = wordVector[i] = topicVector[i] = 0;
        }
        topicCounts = new int[T];
        for (int i = 0; i < T; ++i) {
            topicCounts[i] = 0;
        }
        topicByDocumentCounts = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            topicByDocumentCounts[i] = 0;
        }
        wordByTopicCounts = new int[W * T];
        for (int i = 0; i < W * T; ++i) {
            wordByTopicCounts[i] = 0;
        }

        int tcount = 0, docs = 0;
        for (ArrayList<Integer> doc : docSet) {
            int offset = 0;
            for (int idx : doc) {
                wordVector[tcount + offset] = idx;
                documentVector[tcount + offset] = docs;
                offset += 1;
            }
            tcount += doc.size();
            docs += 1;
        }
    }

    /**
     * Takes commandline options and initializes/assigns model parameters
     * 
     * @param options Default options and options from the command line
     */
    protected void initializeFromOptions(CommandLineOptions options) {
        T = options.getTopics();
        alpha = options.getAlpha();
        beta = options.getBeta();

        int randSeed = options.getRandomSeed();
        if (randSeed == 0) {
            /**
             * Case for complete random seeding
             */
            rand = new MersenneTwisterFast();
        } else {
            /**
             * Case for non-random seeding. For debugging. Also, the default
             */
            rand = new MersenneTwisterFast(randSeed);
        }
        double targetTemp = options.getTargetTemperature();
        double initialTemp = options.getInitialTemperature();
        if (Math.abs(initialTemp - targetTemp) < Constants.EPSILON) {
            annealer = new EmptyAnnealer(options);
        } else {
            annealer = new SimulatedAnnealer(options);
        }

        outputPerClass = options.getOutputPerClass();
        inputPath = options.getTrainInputPath();
        tabulatedOutput = options.getTabulatedOutput();
    }

    /**
     * Randomly initialize fields for training
     */
    public void randomInitialize() {
        int wordid, docid, topicid;
        for (int i = 0; i < N; ++i) {
            wordid = wordVector[i];
            docid = documentVector[i];
            topicid = rand.nextInt(T);

            topicVector[i] = topicid;
            topicCounts[topicid]++;
            topicByDocumentCounts[docid * T + topicid]++;
            wordByTopicCounts[wordid * T + topicid]++;
        }
    }

    /**
     * Train topic model
     *
     * @param annealer Annealing scheme to use
     */
    public void train(Annealer annealer) {
        int wordid, docid, topicid;
        int wordoff, docoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        while (annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                wordid = wordVector[i];
                docid = documentVector[i];
                topicid = topicVector[i];
                docoff = docid * T;
                wordoff = wordid * T;

                topicCounts[topicid]--;
                topicByDocumentCounts[docoff + topicid]--;
                wordByTopicCounts[wordoff + topicid]--;

                try {
                    for (int j = 0;; ++j) {
                        probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                              / (topicCounts[j] + betaW)
                              * (topicByDocumentCounts[docoff + j] + alpha);
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                }
                totalprob = annealer.annealProbs(probs);
                r = rand.nextDouble() * totalprob;

                max = probs[0];
                topicid = 0;
                while (r > max) {
                    topicid++;
                    max += probs[topicid];
                }
                topicVector[i] = topicid;

                topicCounts[topicid]++;
                topicByDocumentCounts[docoff + topicid]++;
                wordByTopicCounts[wordoff + topicid]++;
            }
        }
    }

    /**
     * 
     */
    @Override
    public void train() {
        randomInitialize();
        System.err.println(String.format("Beginning training with %d tokens, %d words, %d regions, %d documents", N, W, T, D));
        train(annealer);
    }

    /**
     * Create two normalized probability tables, {@link #TopWordsPerTopic} and
     * {@link #topicProbs}. {@link #topicProbs} overwrites previous values.
     * {@link #TopWordsPerTopic} only retains first {@link #outputPerTopic}
     * words and values.
     */
    public void normalize() {
        topWordsPerTopic = new StringDoublePair[T][];
        for (int i = 0; i < T; ++i) {
            topWordsPerTopic[i] = new StringDoublePair[outputPerClass];
        }

        Double sum = 0.;
        for (int i = 0; i < T; ++i) {
            sum += topicProbs[i] = topicCounts[i] + betaW;
            ArrayList<DoubleStringPair> topWords = new ArrayList<DoubleStringPair>();
            for (int j = 0; j < W; ++j) {
                topWords.add(
                      new DoubleStringPair(wordByTopicCounts[j * T + i] + beta,
                      docSet.getWordForInt(j)));
            }
            Collections.sort(topWords);
            for (int j = 0; j < outputPerClass; ++j) {
                topWordsPerTopic[i][j] = new StringDoublePair(
                      topWords.get(j).stringValue, topWords.get(j).doubleValue
                      / topicProbs[i]);
            }
        }

        for (int i = 0; i < T; ++i) {
            topicProbs[i] /= sum;
        }
    }

    /**
     * Print the normalized sample counts to tabulatedOutput. Print only the top {@link
     * #outputPerClass} per given topic.
     *
     * @throws IOException
     */
    public void printTabulatedProbabilities() throws
          IOException {
        printTopics(tabulatedOutput);
    }

    /**
     * Print the normalized sample counts to out. Print only the top {@link
     * #outputPerClass} per given topic.
     *
     * @param out Output buffer to write to.
     * @throws IOException
     */
    public void printTabulatedProbabilities(BufferedWriter out) throws
          IOException {
        printTopics(out);
    }

    /**
     * Print the normalized sample counts for each topic to out. Print only the top {@link
     * #outputPerTopic} per given topic.
     *
     * @param out
     * @throws IOException
     */
    protected void printTopics(BufferedWriter out) throws IOException {
        int startt = 0, M = 4, endt = Math.min(M + startt, topicProbs.length);
        out.write("***** Word Probabilities by Topic *****\n\n");
        while (startt < T) {
            for (int i = startt; i < endt; ++i) {
                String header = "T_" + i;
                header = String.format("%25s\t%6.5f\t", header, topicProbs[i]);
                out.write(header);
            }

            out.newLine();
            out.newLine();

            for (int i = 0;
                  i < outputPerClass; ++i) {
                for (int c = startt; c < endt; ++c) {
                    String line = String.format("%25s\t%6.5f\t",
                          topWordsPerTopic[c][i].stringValue,
                          topWordsPerTopic[c][i].doubleValue);
                    out.write(line);
                }
                out.newLine();
            }
            out.newLine();
            out.newLine();

            startt = endt;
            endt = java.lang.Math.min(T, startt + M);
        }
    }
}
