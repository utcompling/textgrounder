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

package opennlp.rlda.models;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class RegionModel {
    /**
     * Vector of toponyms. If 0, the word is not a toponym. If 1, it is.
     */
    protected transient int[] toponymVector;
    /**
     * Vector of stopwords. If 0, the word is not a stopword. If 1, it is.
     */
    protected transient int[] stopwordVector;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByTopicCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected int[] regionByToponymFilter;
    /**
     *
     */
    protected int evalIterations;
    /**
     *
     */
    protected int[] wordIdMapper;
    /**
     *
     */
    protected int[] activeRegionByDocumentFilter;

    /**
     * Default constructor. Take input from commandline and default _options
     * and initialize class. Also, process input text and process so that
     * toponyms, stopwords and other words are identified and collected.
     *
     * @param _options
     */
    public RegionModel(CommandLineOptions _options) {
        try {
            initialize(_options);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(RegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(RegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(RegionModel.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(RegionModel.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     */
    public RegionModel() {
    }

    /**
     *
     * @param _options
     */
    @Override
    protected void initialize(CommandLineOptions _options) throws
          FileNotFoundException, IOException, ClassNotFoundException,
          SQLException {

        modelIterations = _options.getModelIterations();

        initializeFromOptions(_options);

        setAllocateRegions(trainTokenArrayBuffer);
    }

    /**
     *
     */
    protected void setAllocateRegions(TokenArrayBuffer _tokenArrayBuffer) {
        N = _tokenArrayBuffer.size();

        /**
         * There is no need to initialize the topicVector. It will be randomly
         * initialized
         */
        topicVector = new int[N];
        wordVector = _tokenArrayBuffer.wordVector;
        documentVector = _tokenArrayBuffer.documentVector;
        toponymVector = _tokenArrayBuffer.toponymVector;
        stopwordVector = _tokenArrayBuffer.stopwordVector;

        System.err.println();
        System.err.print("Buildng vocabulary index mapping from old lexicon to local lexicon for docs: ");
        int curDoc = 0, prevDoc = -1;

        TIntHashSet inputDataVocab = new TIntHashSet();
        int inputVocabMaxId = 0;
        for (int i = 0; i < N; i++) {
            curDoc = documentVector[i];
            if (curDoc != prevDoc) {
                System.err.print(curDoc + ",");
            }
            prevDoc = curDoc;
            int wordid = wordVector[i];
            inputDataVocab.add(wordid);
            if (wordid > inputVocabMaxId) {
                inputVocabMaxId = wordid;
            }
        }
        inputVocabMaxId += 1;
        wordIdMapper = new int[inputVocabMaxId];
        for (int i = 0; i < inputVocabMaxId; ++i) {
            wordIdMapper[i] = -1;
        }
        int counter = 0;
        for (int wordid : inputDataVocab.toArray()) {
            wordIdMapper[wordid] = counter;
            counter += 1;
        }

        System.err.println();
        System.err.print("Building lookup tables for locations, regions and toponyms for document: ");
        /**
         * Here, find
         * <li>
         */
        TIntHashSet toponymsNotInGazetteer = new TIntHashSet();

        for (int i = 0; i < N; i++) {
            curDoc = documentVector[i];
            if (curDoc != prevDoc) {
                System.err.print(curDoc + ",");
            }
            prevDoc = curDoc;
            int originalwordid = wordVector[i];
            if (toponymVector[i] == 1) {
                if (dataSpecificGazetteer.contains(originalwordid)) {
                    TIntHashSet possibleLocations = dataSpecificGazetteer.get(originalwordid);
                    for (int locid : possibleLocations.toArray()) {
                        E loc = dataSpecificLocationMap.get(locid);
                        addLocationsToRegionArray(loc, regionMapperCallback);
                    }
                    /**
                     * IMPORTANT: THESE STRUCTURES ARE REFERENCED BY THEIR ORIGINAL WORDIDS
                     */
                    regionMapperCallback.addAll(originalwordid);
                    locationSet.addAll(possibleLocations.toArray());
                } else {
                    /**
                     * IMPORTANT: THESE STRUCTURES ARE REFERENCED BY THEIR ORIGINAL WORDIDS
                     */
                    toponymsNotInGazetteer.add(originalwordid);
                }
            }
        }
        System.err.println();
        System.err.println("Reducing size of region mapper callback objects");
        regionMapperCallback.trimToSize();

        D = _tokenArrayBuffer.getNumDocs();
        /**
         * Here we distinguish between the full dictionary size (fW) and the
         * dictionary size without stopwords (W). Normalization is conducted with
         * the dictionary size without stopwords, the size of which is sW.
         */
        fW = inputDataVocab.size();
        sW = stopwordList.size();
        W = fW - sW;
        betaW = beta * W;

        T = regionMapperCallback.getNumRegions();
        topicCounts = new int[T];
        for (int i = 0; i < T; ++i) {
            topicCounts[i] = 0;
        }
        topicByDocumentCounts = new int[D * T];
        activeRegionByDocumentFilter = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            activeRegionByDocumentFilter[i] = topicByDocumentCounts[i] = 0;
        }
        wordByTopicCounts = new int[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            wordByTopicCounts[i] = 0;
        }
        regionByToponymFilter = new int[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            regionByToponymFilter[i] = 0;
        }

        /**
         * Build active regions by document filter
         */
        System.err.print("Building active regions by document filter for docs: ");
        TIntObjectHashMap<TIntHashSet> placenameIdxToRegionIndexSet = regionMapperCallback.getPlacenameIdxToRegionIndexSet();
        for (int i = 0; i < N; i++) {
            int docid = curDoc = documentVector[i];
            if (curDoc != prevDoc) {
                System.err.print(curDoc + ",");
            }
            prevDoc = curDoc;

            int docoff = docid * T;
            int originalwordid = wordVector[i];
            if (toponymVector[i] == 1) {
                if (dataSpecificGazetteer.contains(originalwordid)) {
                    try {
                        for (int regionid :
                              placenameIdxToRegionIndexSet.get(originalwordid).toArray()) {
                            activeRegionByDocumentFilter[docoff + regionid] = 1;
                        }
                    } catch (NullPointerException e) {
                        System.err.println("NullPointerException occurred with (why?): " + lexicon.getWordForInt(originalwordid));
                    }
                }
            }
        }
        System.err.println();

        /**
         * build filters for regionByToponymFilter
         */
        System.err.println("Building filters for regionByToponymFilter");
        for (int wordid : placenameIdxToRegionIndexSet.keys()) {
            int wordoff = wordIdMapper[wordid] * T;
            if (wordoff < 0) {
                System.err.println(lexicon.getWordForInt(wordid) + "is not in local data set. Why?");
            } else {
                for (int regionid :
                      placenameIdxToRegionIndexSet.get(wordid).toArray()) {
                    regionByToponymFilter[wordoff + regionid] = 1;
                }
            }
        }

        for (int wordid : toponymsNotInGazetteer.toArray()) {
            int wordoff = wordIdMapper[wordid] * T;
            if (wordoff < 0) {
                System.err.println(lexicon.getWordForInt(wordid) + "is not in local data set. Why?");
            } else {
                for (int i = 0; i < T; ++i) {
                    regionByToponymFilter[wordoff + i] = 1;
                }
            }
        }
    }

    /**
     * Randomly initialize fields for training. If word is a toponym, choose
     * random region only from regions aligned to name.
     */
    @Override
    public void randomInitialize() {
        int wordid, docid, topicid;
        int istoponym, isstopword;
        int wordoff, docoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordIdMapper[wordVector[i]];
                docid = documentVector[i];
                docoff = docid * T;
                istoponym = toponymVector[i];

                totalprob = 0;
                if (istoponym == 1) {
                    wordoff = wordid * T;
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] =
                                  regionByToponymFilter[wordoff + j]
                                  * activeRegionByDocumentFilter[docoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                } else {
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] =
                                  activeRegionByDocumentFilter[docoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                }

                r = rand.nextDouble() * totalprob;

                max = probs[0];
                topicid = 0;
                while (r > max) {
                    topicid++;
                    max += probs[topicid];
                }

                topicVector[i] = topicid;
                topicCounts[topicid]++;
                topicByDocumentCounts[docid * T + topicid]++;
                wordByTopicCounts[wordid * T + topicid]++;
            }
        }
    }

    /**
     * Train topics
     *
     * @param annealer Annealing scheme to use
     */
    @Override
    public void train(Annealer annealer) {
        int wordid, docid, topicid;
        int wordoff, docoff;
        int istoponym, isstopword;
        double[] probs = new double[T];
        double totalprob, max, r;

        while (annealer.nextIter()) {
            for (int i = 0; i < N; ++i) {
                isstopword = stopwordVector[i];
                if (isstopword == 0) {
                    wordid = wordIdMapper[wordVector[i]];
                    docid = documentVector[i];
                    topicid = topicVector[i];
                    istoponym = toponymVector[i];
                    docoff = docid * T;
                    wordoff = wordid * T;

                    topicCounts[topicid]--;
                    topicByDocumentCounts[docoff + topicid]--;
                    wordByTopicCounts[wordoff + topicid]--;

                    try {
                        if (istoponym == 1) {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha)
                                      * regionByToponymFilter[wordoff + j]
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha)
                                      * activeRegionByDocumentFilter[docoff + j];
                            }
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

            annealer.collectSamples(topicCounts, wordByTopicCounts);
        }
    }

    /**
     * Remove stopwords from normalization process
     */
    @Override
    public void normalize() {
        normalize(stopwordList);
    }

    /**
     *
     */
    @Override
    public void evaluate() {
        EvalRegionModel evalRegionModel = new EvalRegionModel(this);
        evalRegionModel.train();
        evalRegionModel.normalizeLocations();
        evalRegionModel.evaluate();
    }

    /**
     * Print the normalized sample counts to tabularOutput. Print only the top {@link
     * #outputPerClass} per given topic.
     *
     * @throws IOException
     */
    @Override
    public void printTabulatedProbabilities() throws
          IOException {
        super.printTabulatedProbabilities();
        writeRegionWordDistributionKMLFile(trainInputPath, tabularOutputFilename);
        saveSimpleParameters(tabularOutputFilename);
    }

    /**
     * Print the normalized sample counts for each topic to out. Print only the top {@link
     * #outputPerTopic} per given topic.
     *
     * @param out
     * @throws IOException
     */
    @Override
    protected void printTopics(BufferedWriter out) throws IOException {
        int startt = 0, M = 4, endt = Math.min(M + startt, topicProbs.length);
        out.write("***** Word Probabilities by Topic *****\n\n");

        regionMap = regionMapperCallback.getIdxToRegionMap();

        while (startt < T) {
            for (int i = startt; i < endt; ++i) {

                double lat = regionMap.get(i).centLat;
                double lon = regionMap.get(i).centLon;

                String header = "T_" + i;
                header = String.format("%25s\t%6.5f\t", String.format("%s (%.0f,%.0f)", header, lat, lon), topicProbs[i]);
                out.write(header);
            }

            out.newLine();
            out.newLine();

            for (int i = 0; i < outputPerClass; ++i) {
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

    /**
     *
     * @param outputFilename
     * @throws IOException
     */
    public void saveSimpleParameters(String outputFilename) throws IOException {
        SerializableRegionParameters sp = new SerializableRegionParameters();
        sp.saveParameters(outputFilename, this);
    }

    /**
     *
     * @param inputFilename
     * @throws IOException
     */
    public void loadSimpleParameters(String inputFilename) throws IOException {
        SerializableRegionParameters sp = new SerializableRegionParameters();
        sp.loadParameters(inputFilename, this);
    }
}
