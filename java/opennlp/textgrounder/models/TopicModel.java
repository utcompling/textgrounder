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

import opennlp.textgrounder.ec.util.MersenneTwisterFast;
import opennlp.textgrounder.geo.DocumentSet;

/**
 * Basic topic model implementation.
 *
 * @author tsmoon
 */
public class TopicModel {

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
     * Counts of words per topic. However, since access more often occurs in
     * terms of the words, it will be a topic by word matrix.
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
     * 
     */
    protected DocumentSet docSet;

    /**
     * This is not the default constructor. It should only be called by
     * constructors of derived classes.
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
    public TopicModel(DocumentSet docSet, int T) {
        this.T = T;
        this.docSet = docSet;
        rand = new MersenneTwisterFast(0);
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

    public void train() {
        int wordid, docid, topicid;
        int wordoff, docoff;
        for (int i = 0; i < N; ++i) {
            wordid = wordVector[i];
            docid = documentVector[i];
            topicid = topicVector[i];


            topicCounts[topicid]--;
            topicByDocumentCounts[docid * T + topicid]--;
            wordByTopicCounts[wordid * T + topicid]--;
        }
    }
}
