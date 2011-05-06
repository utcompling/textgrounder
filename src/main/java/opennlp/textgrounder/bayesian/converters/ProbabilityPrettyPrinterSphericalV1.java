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

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;
import opennlp.textgrounder.bayesian.apps.*;
import opennlp.textgrounder.bayesian.structs.IntDoublePair;
import opennlp.textgrounder.bayesian.mathutils.TGMath;
import opennlp.textgrounder.bayesian.structs.AveragedSphericalCountWrapper;
import opennlp.textgrounder.bayesian.topostructs.Coordinate;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import opennlp.textgrounder.bayesian.spherical.io.*;

/**
 *
 * @author Taesun Moon <tsunmoon@gmail.com>
 */
public class ProbabilityPrettyPrinterSphericalV1 extends ProbabilityPrettyPrinter {

    /**
     *
     */
    protected int T;
    /**
     * 
     */
    protected int Z;
    /**
     *
     */
    protected double kappa;
    /**
     *
     */
    protected HashSet<Integer> emptyRSet;
    /**
     *
     */
    protected double[][][] toponymCoordinateLexicon;
    /**
     *
     */
    protected double[] averagedWordByRegionCounts;
    /**
     *
     */
    protected double[] averagedRegionCountsOfAllWords;
    /**
     *
     */
    protected double[] averagedRegionByDocumentCounts;
    /**
     *
     */
    protected double[][] averagedRegionMeans;
    /**
     *
     */
    protected double[][][] averagedRegionToponymCoordinateCounts;
    /**
     *
     */
    protected int[] nonEmptyRArray;
    /**
     *
     */
    protected SphericalInternalToInternalInputReader sphericalInputReader = null;
    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////
    /**
     *
     */
    private double[] averagedWordByTopicCounts;
    /**
     *
     */
    private double[] averagedToponymByRegionCounts;

    public ProbabilityPrettyPrinterSphericalV1(ConverterExperimentParameters _parameters) {
        super(_parameters);
        sphericalInputReader = new SphericalInternalToInternalBinaryInputReader(experimentParameters);
    }

    @Override
    public void readFiles() {
        AveragedSphericalCountWrapper averagedCountWrapper = sphericalInputReader.readProbabilities();

        alpha = averagedCountWrapper.getAlpha();
//        beta = averagedCountWrapper.getBeta();
//        betaW = averagedCountWrapper.getBetaW();
//        kappa = averagedCountWrapper.getKappa();

        D = averagedCountWrapper.getD();
        N = averagedCountWrapper.getN();
        L = averagedCountWrapper.getL();
        W = averagedCountWrapper.getW();
        T = averagedCountWrapper.getT();

        nonEmptyRArray = new int[L - emptyRSet.size()];
        for (int count = 0, i = 0; i < L; ++i) {
            if (!emptyRSet.contains(i)) {
                nonEmptyRArray[count] = i;
                count += 1;
            }
        }

        averagedRegionMeans = averagedCountWrapper.getRegionMeansFM();
//        averagedRegionToponymCoordinateCounts = averagedCountWrapper.getAveragedRegionToponymCoordinateCounts();
        averagedRegionByDocumentCounts = averagedCountWrapper.getKappaFM();
        averagedToponymByRegionCounts = averagedWordByRegionCounts = averagedCountWrapper.getGlobalDishWeightsFM();

        lexicon = inputReader.readLexicon();
    }

    @Override
    public void normalizeAndPrintXMLProbabilities() {
        int outputPerClass = experimentParameters.getOutputPerClass();
        String outputPath = experimentParameters.getXmlConditionalProbabilitiesPath();

        try {

            XMLOutputFactory factory = XMLOutputFactory.newInstance();
            XMLStreamWriter w = factory.createXMLStreamWriter(new BufferedWriter(new FileWriter(outputPath)));

            String rootName = "probabilities";
            String wordByRegionName = "toponym-by-region";
            String regionByWordName = "region-by-toponym";
            String regionByDocumentName = "region-by-document";
            w.writeStartDocument("UTF-8", "1.0");
            w.writeStartElement(rootName);

            {
                w.writeStartElement(wordByRegionName);

                double sum = 0.;
                double[] averagedRegionCounts = new double[L];
                Arrays.fill(averagedRegionCounts, 0);
                for (int i : nonEmptyRArray) {
                    for (int j = 0; j < T; ++j) {
                        sum += averagedToponymByRegionCounts[j * L + i] + beta;
                        averagedRegionCounts[i] += averagedToponymByRegionCounts[j * L + i] + beta;
                    }
                }

                for (int i : nonEmptyRArray) {
                    ArrayList<IntDoublePair> topWords = new ArrayList<IntDoublePair>();
                    for (int j = 0; j < T; ++j) {
                        topWords.add(new IntDoublePair(j, averagedToponymByRegionCounts[j * L + i] + beta));
                    }
                    Collections.sort(topWords);

                    Coordinate coord = new Coordinate(TGMath.cartesianToGeographic(TGMath.normalizeVector(averagedRegionMeans[i])));
                    w.writeStartElement("region");

                    w.writeAttribute("id", String.format("%04d", i));
                    w.writeAttribute("lat", String.format("%.6f", coord.latitude));
                    w.writeAttribute("lon", String.format("%.6f", coord.longitude));
                    w.writeAttribute("prob", String.format("%.8e", averagedRegionCounts[i] / sum));

                    for (int j = 0; j < outputPerClass; ++j) {
                        w.writeStartElement("word");

                        IntDoublePair pair = topWords.get(j);
                        if (isInvalidProb(pair.count / averagedRegionCounts[i])) {
                            break;
                        }
                        w.writeAttribute("term", lexicon.getWordForInt(pair.index));
                        w.writeAttribute("prob", String.format("%.8e", pair.count / averagedRegionCounts[i]));
                        w.writeEndElement();
                    }
                    w.writeEndElement();
                }
                w.writeEndElement();
            }

            {
                w.writeStartElement(regionByWordName);

                double[] wordCounts = new double[T];

                for (int i = 0; i < T; ++i) {
                    wordCounts[i] = 0;
                    int wordoff = i * L;
                    for (int j : nonEmptyRArray) {
                        wordCounts[i] += averagedToponymByRegionCounts[wordoff + j] + beta;
                    }
                }


                for (int i = 0; i < T; ++i) {
                    int wordoff = i * L;
                    ArrayList<IntDoublePair> topRegions = new ArrayList<IntDoublePair>();
                    for (int j : nonEmptyRArray) {
                        topRegions.add(new IntDoublePair(j, averagedToponymByRegionCounts[wordoff + j] + beta));
                    }
                    Collections.sort(topRegions);

                    w.writeStartElement("word");
                    w.writeAttribute("term", lexicon.getWordForInt(i));

                    for (int j = 0; j < outputPerClass; ++j) {
                        w.writeStartElement("region");

                        IntDoublePair pair = topRegions.get(j);
                        if (isInvalidProb(pair.count / wordCounts[i])) {
                            break;
                        }
                        Coordinate coord = new Coordinate(TGMath.cartesianToGeographic(TGMath.normalizeVector(averagedRegionMeans[pair.index])));
                        w.writeAttribute("id", String.format("%04d", pair.index));
                        w.writeAttribute("lat", String.format("%.6f", coord.latitude));
                        w.writeAttribute("lon", String.format("%.6f", coord.longitude));
                        w.writeAttribute("kappa", String.format("%.2f", kappa));
                        w.writeAttribute("prob", String.format("%.8e", pair.count / wordCounts[i]));
                        w.writeEndElement();
                    }
                    w.writeEndElement();
                }
                w.writeEndElement();
            }

            /**
             * print region by document probabilities
             */
            {
                Document trdoc = null;
                try {
                    trdoc = (new SAXBuilder()).build(experimentParameters.getInputPath());
                } catch (JDOMException ex) {
                    Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                } catch (IOException ex) {
                    Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
                    System.exit(1);
                }

                HashMap<Integer, String> docidToName = new HashMap<Integer, String>();
                int docid = 0;
                Element trroot = trdoc.getRootElement();
                ArrayList<Element> documents = new ArrayList<Element>(trroot.getChildren());
                for (Element document : documents) {
                    String docidName = document.getAttributeValue("id");
                    if (docidName == null) {
                        docidName = String.format("doc%06d", docid);
                    }
                    docidToName.put(docid, docidName);
                    docid += 1;
                }

                w.writeStartElement(regionByDocumentName);

                double[] docWordCounts = new double[D];

                for (int i = 0; i < D; ++i) {
                    docWordCounts[i] = 0;
                    int docoff = i * L;
                    for (int j : nonEmptyRArray) {
                        docWordCounts[i] += averagedRegionByDocumentCounts[docoff + j] + alpha;
                    }
                }

                for (int i = 0; i < D; ++i) {
                    int docoff = i * L;
                    ArrayList<IntDoublePair> topRegions = new ArrayList<IntDoublePair>();
                    for (int j : nonEmptyRArray) {
                        topRegions.add(new IntDoublePair(j, averagedRegionByDocumentCounts[docoff + j] + alpha));
                    }
                    Collections.sort(topRegions);

                    w.writeStartElement("document");
                    w.writeAttribute("id", docidToName.get(i));

                    for (int j = 0; j < outputPerClass; ++j) {
                        w.writeStartElement("region");

                        IntDoublePair pair = topRegions.get(j);
                        if (isInvalidProb(pair.count / docWordCounts[i])) {
                            break;
                        }
                        Coordinate coord = new Coordinate(TGMath.cartesianToGeographic(TGMath.normalizeVector(averagedRegionMeans[pair.index])));
                        w.writeAttribute("id", String.format("%04d", pair.index));
                        w.writeAttribute("lat", String.format("%.6f", coord.latitude));
                        w.writeAttribute("lon", String.format("%.6f", coord.longitude));
                        w.writeAttribute("kappa", String.format("%.2f", kappa));
                        w.writeAttribute("prob", String.format("%.8e", pair.count / docWordCounts[i]));
                        w.writeEndElement();
                    }
                    w.writeEndElement();
                }
                w.writeEndElement();
            }

            w.writeEndElement();
            w.close();

        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
        } catch (XMLStreamException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Only for toponyms
     */
    @Override
    public void normalizeAndPrintWordByRegion() {
        try {
            String toponymByRegionFilename = experimentParameters.getWordByRegionProbabilitiesPath();
            BufferedWriter toponymByRegionWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(toponymByRegionFilename))));

            double sum = 0.;
            double[] averagedRegionCounts = new double[L];
            Arrays.fill(averagedRegionCounts, 0);
            for (int i : nonEmptyRArray) {
                for (int j = 0; j < T; ++j) {
                    sum += averagedToponymByRegionCounts[j * L + i] + beta;
                    averagedRegionCounts[i] += averagedToponymByRegionCounts[j * L + i] + beta;
                }
            }

            for (int i : nonEmptyRArray) {
                ArrayList<IntDoublePair> topWords = new ArrayList<IntDoublePair>();
                for (int j = 0; j < T; ++j) {
                    topWords.add(new IntDoublePair(j, averagedToponymByRegionCounts[j * L + i] + beta));
                }
                Collections.sort(topWords);

                Coordinate coord = new Coordinate(TGMath.cartesianToGeographic(TGMath.normalizeVector(averagedRegionMeans[i])));
                toponymByRegionWriter.write(String.format("Region%04d\t%.6f\t%.6f\t%.2f\t%.8e", i, coord.longitude, coord.latitude, kappa, averagedRegionCounts[i] / sum));
                toponymByRegionWriter.newLine();
                for (IntDoublePair pair : topWords) {
                    if (isInvalidProb(pair.count / averagedRegionCounts[i])) {
                        break;
                    }
                    toponymByRegionWriter.write(String.format("%s\t%.8e", lexicon.getWordForInt(pair.index), pair.count / averagedRegionCounts[i]));
                    toponymByRegionWriter.newLine();
                }
                toponymByRegionWriter.newLine();
            }

            toponymByRegionWriter.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    /**
     * Only for toponyms
     */
    @Override
    public void normalizeAndPrintRegionByWord() {
        try {
            String regionByToponymFilename = experimentParameters.getRegionByWordProbabilitiesPath();
            BufferedWriter regionByToponymWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(regionByToponymFilename))));

            double[] wordCounts = new double[T];

            for (int i = 0; i < T; ++i) {
                wordCounts[i] = 0;
                int wordoff = i * L;
                for (int j : nonEmptyRArray) {
                    wordCounts[i] += averagedToponymByRegionCounts[wordoff + j] + beta;
                }
            }

            for (int i = 0; i < T; ++i) {
                int wordoff = i * L;
                ArrayList<IntDoublePair> topRegions = new ArrayList<IntDoublePair>();
                for (int j : nonEmptyRArray) {
                    topRegions.add(new IntDoublePair(j, averagedToponymByRegionCounts[wordoff + j] + beta));
                }
                Collections.sort(topRegions);

                regionByToponymWriter.write(String.format("%s", lexicon.getWordForInt(i)));
                regionByToponymWriter.newLine();
                for (IntDoublePair pair : topRegions) {
                    if (isInvalidProb(pair.count / wordCounts[i])) {
                        break;
                    }
                    Coordinate coord = new Coordinate(TGMath.cartesianToGeographic(TGMath.normalizeVector(averagedRegionMeans[pair.index])));
                    regionByToponymWriter.write(String.format("%04d\t%.6f\t%.6f\t%.2f\t%.8e", pair.index, coord.longitude, coord.latitude, kappa, pair.count / wordCounts[i]));
                    regionByToponymWriter.newLine();
                }
                regionByToponymWriter.newLine();
            }

            regionByToponymWriter.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    /**
     *
     */
    @Override
    public void normalizeAndPrintRegionByDocument() {
        try {
            String regionByDocumentFilename = experimentParameters.getRegionByDocumentProbabilitiesPath();
            BufferedWriter regionByDocumentWriter = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(regionByDocumentFilename))));

            SAXBuilder builder = new SAXBuilder();
            Document trdoc = null;
            try {
                trdoc = builder.build(experimentParameters.getInputPath());
            } catch (JDOMException ex) {
                Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(1);
            } catch (IOException ex) {
                Logger.getLogger(XMLToInternalConverter.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(1);
            }

            HashMap<Integer, String> docidToName = new HashMap<Integer, String>();
            int docid = 0;
            Element root = trdoc.getRootElement();
            ArrayList<Element> documents = new ArrayList<Element>(root.getChildren());
            for (Element document : documents) {
                docidToName.put(docid, document.getAttributeValue("id"));
                docid += 1;
            }

            double[] docWordCounts = new double[D];

            for (int i = 0; i < D; ++i) {
                docWordCounts[i] = 0;
                int docoff = i * L;
                for (int j : nonEmptyRArray) {
                    docWordCounts[i] += averagedRegionByDocumentCounts[docoff + j] + alpha;
                }
            }

            for (int i = 0; i < D; ++i) {
                int docoff = i * L;
                ArrayList<IntDoublePair> topRegions = new ArrayList<IntDoublePair>();
                for (int j : nonEmptyRArray) {
                    topRegions.add(new IntDoublePair(j, averagedRegionByDocumentCounts[docoff + j] + alpha));
                }
                Collections.sort(topRegions);

                regionByDocumentWriter.write(String.format("%s", docidToName.get(i)));
                regionByDocumentWriter.newLine();
                for (IntDoublePair pair : topRegions) {
                    if (isInvalidProb(pair.count / docWordCounts[i])) {
                        break;
                    }
                    Coordinate coord = new Coordinate(TGMath.cartesianToGeographic(TGMath.normalizeVector(averagedRegionMeans[pair.index])));
                    regionByDocumentWriter.write(String.format("%04d\t%.6f\t%.6f\t%.2f\t%.8e", pair.index, coord.longitude, coord.latitude, kappa, pair.count / docWordCounts[i]));
                    regionByDocumentWriter.newLine();
                }
                regionByDocumentWriter.newLine();
            }

            regionByDocumentWriter.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        } catch (IOException ex) {
            Logger.getLogger(ProbabilityPrettyPrinterSphericalV2.class.getName()).log(Level.SEVERE, null, ex);
            System.exit(1);
        }
    }

    public void normalizeAndPrintWordByTopic() {
        return;
    }

    @Override
    public void normalizeAndPrintAll() {
        super.normalizeAndPrintAll();
        normalizeAndPrintWordByTopic();
    }
}
