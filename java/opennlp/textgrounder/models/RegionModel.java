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
import opennlp.textgrounder.gazetteers.Gazetteer;
import opennlp.textgrounder.geo.*;
import opennlp.textgrounder.models.callbacks.*;
import opennlp.textgrounder.textstructs.*;
import opennlp.textgrounder.topostructs.*;
import opennlp.textgrounder.util.Constants;
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
    protected int[] regionByToponym;
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
     * Default constructor. Take input from commandline and default options
     * and initialize class. Also, process input text and process so that
     * toponyms, stopwords and other words are identified and collected.
     *
     * @param options
     */
    public RegionModel(CommandLineOptions options) {
        regionMapperCallback = new RegionMapperCallback();
        try {
            initialize(options);
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
     * @param options
     */
    @Override
    protected void initialize(CommandLineOptions options) throws
          FileNotFoundException, IOException, ClassNotFoundException,
          SQLException {
        super.initialize(options);
        initializeFromOptions(options);

        TextProcessor textProcessor = null;
        String fname = trainInputFile.getName();
        if (options.isPCLXML()) {
            textProcessor = new TextProcessorTEIXML(lexicon);
        } else if (trainInputFile.isDirectory() && trainInputFile.list(new PCLXMLFilter()).length != 0) {
            textProcessor = new TextProcessorTEIXML(lexicon);
        } else if (fname.startsWith("txu") && fname.endsWith(".xml")) {
            textProcessor = new TextProcessorTEIXML(lexicon);
        } else {
            textProcessor = new TextProcessor(lexicon, paragraphsAsDocs);
        }

        trainTokenArrayBuffer = new TokenArrayBuffer(lexicon, new TrainingMaterialCallback(lexicon));
        stopwordList = new StopwordList();
        processTrainInputPath(trainInputFile, textProcessor, trainTokenArrayBuffer, stopwordList);
        trainTokenArrayBuffer.convertToPrimitiveArrays();
        initializeRegionArray();
        locationSet = new TIntHashSet();
        setAllocateRegions(trainTokenArrayBuffer);
    }

    /**
     *
     */
    protected void setAllocateRegions(TokenArrayBuffer tokenArrayBuffer) {
        N = tokenArrayBuffer.size();
        /**
         * Here we distinguish between the full dictionary size (fW) and the
         * dictionary size without stopwords (W). Normalization is conducted with
         * the dictionary size without stopwords, the size of which is sW.
         */
        sW = stopwordList.size();
        fW = lexicon.getDictionarySize();
        W = fW - sW;
        D = tokenArrayBuffer.getNumDocs();
        betaW = beta * W;

        wordVector = tokenArrayBuffer.wordVector;
        documentVector = tokenArrayBuffer.documentVector;
        toponymVector = tokenArrayBuffer.toponymVector;
        stopwordVector = tokenArrayBuffer.stopwordVector;

        /**
         * There is no need to initialize the topicVector. It will be randomly
         * initialized
         */
        topicVector = new int[N];

        TIntHashSet toponymsNotInGazetteer = buildTopoTable();

        T = regionMapperCallback.getNumRegions();
        topicCounts = new int[T];
        for (int i = 0; i < T; ++i) {
            topicCounts[i] = 0;
        }
        topicByDocumentCounts = new int[D * T];
        for (int i = 0; i < D * T; ++i) {
            topicByDocumentCounts[i] = 0;
        }
        wordByTopicCounts = new int[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            wordByTopicCounts[i] = 0;
        }
        regionByToponym = new int[fW * T];
        for (int i = 0; i < fW * T; ++i) {
            regionByToponym[i] = 0;
        }

        buildTopoFilter(toponymsNotInGazetteer);
    }

    /**
     * 
     * @return
     */
    protected TIntHashSet buildTopoTable() {
        System.err.println();
        System.err.print("Buildng lookup tables for locations, regions and toponyms for document: ");
        int curDoc = 0, prevDoc = -1;

        TIntHashSet toponymsNotInGazetteer = new TIntHashSet();
        Gazetteer<E> gazetteer = gazetteerGenerator.generateGazetteer();
        for (int i = 0; i < N; i++) {
            curDoc = documentVector[i];
            if (curDoc != prevDoc) {
                System.err.print(curDoc + ",");
            }
            prevDoc = curDoc;
            if (toponymVector[i] == 1) {
                String placename = lexicon.getWordForInt(wordVector[i]);
                if (gazetteer.contains(placename)) {
                    TIntHashSet possibleLocations = gazetteer.frugalGet(placename);

                    TIntHashSet tempLocs = new TIntHashSet();
                    for (TIntIterator it = possibleLocations.iterator();
                          it.hasNext();) {
                        int locid = it.next();
                        E loc = gazetteer.getLocation(locid);

                        if (Math.abs(loc.getCoord().latitude) > Constants.EPSILON && Math.abs(loc.getCoord().longitude) > Constants.EPSILON) {
                            tempLocs.add(loc.getId());
                        }
                    }
                    possibleLocations = tempLocs;

                    addLocationsToRegionArray(possibleLocations, gazetteer, regionMapperCallback);
                    regionMapperCallback.addAll(placename, lexicon);
                    locationSet.addAll(possibleLocations.toArray());
                } else {
                    toponymsNotInGazetteer.add(wordVector[i]);
                }
            }
        }
        System.err.println();
        return toponymsNotInGazetteer;
    }

    protected void buildTopoFilter(TIntHashSet toponymsNotInGazetteer) {
        TIntObjectHashMap<TIntHashSet> nameToRegionIndex = regionMapperCallback.getNameToRegionIndex();
        for (TIntObjectIterator<TIntHashSet> it1 = nameToRegionIndex.iterator();
              it1.hasNext();) {
            it1.advance();
            int wordoff = it1.key() * T;
            for (TIntIterator it2 = it1.value().iterator(); it2.hasNext();) {
                int j = it2.next();
                regionByToponym[wordoff + j] = 1;
            }
        }

        for (TIntIterator it = toponymsNotInGazetteer.iterator(); it.hasNext();) {
            int topid = it.next();
            int topoff = topid * T;
            for (int i = 0; i < T; ++i) {
                regionByToponym[topoff + i] = 1;
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
        int wordoff;
        double[] probs = new double[T];
        double totalprob, max, r;

        for (int i = 0; i < N; ++i) {
            isstopword = stopwordVector[i];
            if (isstopword == 0) {
                wordid = wordVector[i];
                docid = documentVector[i];
                istoponym = toponymVector[i];

                if (istoponym == 1) {
                    wordoff = wordid * T;
                    totalprob = 0;
                    try {
                        for (int j = 0;; ++j) {
                            totalprob += probs[j] = regionByToponym[wordoff + j];
                        }
                    } catch (ArrayIndexOutOfBoundsException e) {
                    }
                    r = rand.nextDouble() * totalprob;

                    max = probs[0];
                    topicid = 0;
                    while (r > max) {
                        topicid++;
                        max += probs[topicid];
                    }

                } else {
                    topicid = rand.nextInt(T);
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
                    wordid = wordVector[i];
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
                                      * regionByToponym[wordoff + j];
                            }
                        } else {
                            for (int j = 0;; ++j) {
                                probs[j] = (wordByTopicCounts[wordoff + j] + beta)
                                      / (topicCounts[j] + betaW)
                                      * (topicByDocumentCounts[docoff + j] + alpha);
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
        Gazetteer<E> gazetteer = gazetteerGenerator.generateGazetteer();

        for (TIntIterator it = locationSet.iterator(); it.hasNext();) {
            int locid = it.next();
            E loc = gazetteer.safeGetLocation(locid);
            loc.setCount(loc.getCount() + beta);
            loc.setBackPointers(new ArrayList<Integer>());
        }

        TIntObjectHashMap<TIntHashSet> nameToRegionIndex = regionMapperCallback.getNameToRegionIndex();
        TIntObjectHashMap<TIntHashSet> toponymRegionToLocations = regionMapperCallback.getToponymRegionToLocations();
        for (int wordid : nameToRegionIndex.keys()) {
            for (int regid : nameToRegionIndex.get(wordid).toArray()) {
                ToponymRegionPair trp = new ToponymRegionPair(wordid, regid);
                TIntHashSet locs = toponymRegionToLocations.get(trp.hashCode());
                for (TIntIterator it = locs.iterator(); it.hasNext();) {
                    int locid = it.next();
                    E loc = gazetteer.safeGetLocation(locid);
                    loc.setCount(loc.getCount() + wordByTopicCounts[wordid * T + regid]);
                }
            }
        }

        int wordid, topicid;
        int istoponym, isstopword;

        int newlocid = gazetteer.getMaxLocId();
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
                            E loc = gazetteer.safeGetLocation(locid);
                            loc.getBackPointers().add(i);
                            if (loc.getId() > newlocid) {
                                loc.setCount(loc.getCount() + 1);
                            }
                        }
                    } catch (NullPointerException e) {
                        locs = new TIntHashSet();
                        Region r = regionMapperCallback.getRegionMap().get(topicid);
                        Coordinate coord = new Coordinate(r.centLon, r.centLat);
                        E loc = generateLocation(curlocid, lexicon.getWordForInt(wordid), null, coord, 0, null, 1, wordid);
                        gazetteer.putLocation(loc);
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
        for (TIntObjectIterator<E> it = gazetteer.getIdxToLocationMap().iterator();
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
        return gazetteer.getIdxToLocationMap();
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

        regionMap = regionMapperCallback.getRegionMap();

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
        regionMap = regionMapperCallback.getRegionMap();

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
        idxToLocationMap = normalizeLocations();
        System.err.println("Writing output");
        writeXMLFile(trainInputPath, kmlOutputFilename, idxToLocationMap, locations, trainTokenArrayBuffer);
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
