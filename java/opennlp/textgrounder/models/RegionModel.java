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

import gnu.trove.TIntHashSet;
import gnu.trove.TIntIterator;
import gnu.trove.TIntObjectHashMap;
import gnu.trove.TIntObjectIterator;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import opennlp.textgrounder.annealers.*;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.KMLUtil;

/**
 * Topic model with region awareness. Toponyms are all unigrams. Multiword
 * toponyms are split into space delimited tokens.
 * 
 * @author tsmoon
 */
public class RegionModel<E extends SmallLocation> extends TopicModel<E> {

    /**
     * Callback class for handling mappings between regions, locations and placenames
     */
    protected RegionMapperCallback<E> regionMapperCallback;
    /**
     * Vector of toponyms. If 0, the word is not a toponym. If 1, it is.
     */
    protected int[] toponymVector;
    /**
     * Vector of stopwords. If 0, the word is not a stopword. If 1, it is.
     */
    protected int[] stopwordVector;
    /**
     * An index of toponyms and possible regions. The goal is fast lookup and not
     * frugality with memory. The dimensions are equivalent to the wordByTopicCounts
     * array. Instead of counts, this array is populated with ones and zeros.
     * If a toponym occurs in a certain region, the cell value is one, zero if not.
     */
    protected int[] regionByToponymFilter;
    /**
     * The set of locations that have been observed with the model.
     */
    protected TIntHashSet locationSet;
    /**
     * Table from index to region
     */
    TIntObjectHashMap<Region> regionMap;
    /**
     * Table from index to location
     */
    TIntObjectHashMap<E> dataSpecificLocationMap;
    /**
     * 
     */
    TIntObjectHashMap<TIntHashSet> dataSpecificGazetteer;
    /**
     * 
     */
    protected StopwordList stopwordList;
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
    public RegionModel(CommandLineOptions _options, E _genericsKludgeFactor) {
        regionMapperCallback = new RegionMapperCallback();
        genericsKludgeFactor = _genericsKludgeFactor;
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
        kmlOutputFilename = _options.getKMLOutputFilename();
        degreesPerRegion = _options.getDegreesPerRegion();
        barScale = _options.getBarScale();
        windowSize = _options.getWindowSize();

        initializeFromOptions(_options);

        SerializableRegionTrainingParameters<E> serializableRegionTrainingParameters = new SerializableRegionTrainingParameters<E>();
        serializableRegionTrainingParameters.loadParameters(_options.getSerializedDataParametersFilename(), this);

        stopwordList = new StopwordList();
        initializeRegionArray();
        locationSet = new TIntHashSet();
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
        System.err.print("Buildng lookup tables for locations, regions and toponyms for document: ");
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
     * 
     */
    protected TIntObjectHashMap<E> normalizeLocations() {
        for (TIntIterator it = locationSet.iterator(); it.hasNext();) {
            int locid = it.next();
            E loc = dataSpecificLocationMap.get(locid);
            loc.setCount(loc.getCount() + beta);
            loc.setBackPointers(new ArrayList<Integer>());
        }

        TIntObjectHashMap<TIntHashSet> nameToRegionIndex = regionMapperCallback.getPlacenameIdxToRegionIndexSet();
        TIntObjectHashMap<TIntHashSet> toponymRegionToLocations = regionMapperCallback.getToponymRegionToLocations();
        for (int wordid : nameToRegionIndex.keys()) {
            for (int regid : nameToRegionIndex.get(wordid).toArray()) {
                ToponymRegionPair trp = new ToponymRegionPair(wordid, regid);
                TIntHashSet locs = toponymRegionToLocations.get(trp.hashCode());
                for (TIntIterator it = locs.iterator(); it.hasNext();) {
                    int locid = it.next();
                    E loc = dataSpecificLocationMap.get(locid);
                    loc.setCount(loc.getCount() + wordByTopicCounts[wordIdMapper[wordid] * T + regid]);
                }
            }
        }

        int wordid, topicid;
        int istoponym, isstopword;

        int newlocid = 0;
        for (int locid : dataSpecificLocationMap.keys()) {
            if (locid > newlocid) {
                newlocid = locid;
            }
        }
        int curlocid = newlocid + 1;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                istoponym = toponymVector[i];
                if (istoponym == 1) {
                    wordid = wordVector[i];
                    topicid = topicVector[i];
                    ToponymRegionPair trp = new ToponymRegionPair(wordid, topicid);
                    TIntHashSet locs = toponymRegionToLocations.get(trp.hashCode());
                    try {
                        for (TIntIterator it = locs.iterator(); it.hasNext();) {
                            int locid = it.next();
                            E loc = dataSpecificLocationMap.get(locid);
                            loc.getBackPointers().add(i);
                            if (loc.getId() > newlocid) {
                                loc.setCount(loc.getCount() + 1);
                            }
                        }
                    } catch (NullPointerException e) {
                        locs = new TIntHashSet();
                        Region r = regionMapperCallback.getIdxToRegionMap().get(topicid);
                        Coordinate coord = new Coordinate(r.centLon, r.centLat);
                        E loc = generateLocation(curlocid, lexicon.getWordForInt(wordid), null, coord, 0, null, 1, wordid);
                        dataSpecificLocationMap.put(curlocid, loc);
                        loc.setBackPointers(new ArrayList<Integer>());
                        loc.getBackPointers().add(i);
                        locs.add(loc.getId());
                        toponymRegionToLocations.put(trp.hashCode(), locs);
                        curlocid += 1;
                    }
                }
            }
        }

        locations = new TIntHashSet();
        for (TIntObjectIterator<E> it = dataSpecificLocationMap.iterator();
              it.hasNext();) {
            it.advance();
            E loc = it.value();
            try {
                if (loc.getBackPointers().size() != 0) {
                    locations.add(loc.getId());
                }
            } catch (NullPointerException e) {
            }
        }
        return dataSpecificLocationMap;
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
     * Output word distributions over regions to Google Earth kml file.
     *
     * @param inputFilename
     * @param outputFilename
     * @throws IOException
     */
    public void writeRegionWordDistributionKMLFile(String inputFilename,
          String outputFilename) throws IOException {

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename + ".kml"));

        out.write(KMLUtil.genKMLHeader(inputFilename));
        regionMap = regionMapperCallback.getIdxToRegionMap();

        double radius = .2;

        for (int i = 0; i < T; ++i) {
            double lat = regionMap.get(i).centLat;
            double lon = regionMap.get(i).centLon;
            Coordinate center = new Coordinate(lon, lat);

            for (int j = 0; j < outputPerClass; ++j) {

                Coordinate spiralPoint = center.getNthSpiralPoint(j, .5);

                String word = topWordsPerTopic[i][j].stringValue;
                double height = topWordsPerTopic[i][j].doubleValue * barScale * 50;
                String kmlPolygon = spiralPoint.toKMLPolygon(10, radius, height);
                out.write(KMLUtil.genPolygon("", spiralPoint, radius, kmlPolygon));
                out.write(KMLUtil.genFloatingPlacemark(word, spiralPoint, height));
            }
        }

        out.write("\t\t</Folder>\n\t</Document>\n</kml>");
        out.close();
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

    /**
     * 
     * @param outputFilename
     * @param word
     * @throws IOException
     */
    public void writeWordOverGlobeKML(String outputFilename, String word) throws
          IOException {
        writeWordOverGlobeKML(trainInputPath, outputFilename, word);
    }

    /**
     * 
     * @param word
     * @throws IOException
     */
    public void writeWordOverGlobeKML(String word) throws
          IOException {
        writeWordOverGlobeKML(trainInputPath, word + ".kml", word);
    }

    /**
     * 
     * @param inputFilename
     * @param outputFilename
     * @throws IOException
     */
    public void writeWordOverGlobeKML(String inputFilename,
          String outputFilename, String word) throws IOException {

        int wordid = lexicon.getIntForWord(word);
        if (wordid == 0) {
            System.err.println("\"" + word + "\" is not in the text");
            System.exit(1);
        }

        BufferedWriter out = new BufferedWriter(new FileWriter(outputFilename));

        out.write(KMLUtil.genKMLHeader(inputFilename));

        double radius = 1;
        T = topicProbs.length;

        for (int i = 0; i < T; ++i) {
            double lat = regionMap.get(i).centLat;
            double lon = regionMap.get(i).centLon;
            Coordinate center = new Coordinate(lon, lat);
            double height = wordByTopicProbs[wordid * T + i] * barScale * 20;
            String kmlPolygon = center.toKMLPolygon(10, radius, height);
            out.write(KMLUtil.genPolygon(word, center, radius, kmlPolygon));
        }

        out.write("\t\t</Folder>\n\t</Document>\n</kml>");
        out.close();
    }

    /**
     * Write assignments to Google Earth KML file.
     * 
     * @throws Exception
     */
    public void writeXMLFile() throws Exception {
        System.err.println();
        System.err.println("Counting locations and smoothing");
        normalizeLocations();
        System.err.println("Writing output");
        writeXMLFile(trainInputPath, kmlOutputFilename, dataSpecificLocationMap, locations, trainTokenArrayBuffer);
    }

    protected E generateLocation(int _id, String _name, String _type,
          Coordinate _coord, int _pop, String _container, int _count,
          int _nameid) {
        E locationToAdd;
        if (genericsKludgeFactor instanceof Location) {
            locationToAdd = (E) new Location(_id, _name, _type, _coord, _pop, _container, _count);
        } else {
            locationToAdd = (E) new SmallLocation(_id, _nameid, _coord, _count);
        }
        return locationToAdd;
    }
}
